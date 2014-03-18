/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore;

import com.cloudant.common.CouchConstants;
import com.cloudant.mazha.DocumentRevs;
import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.util.CouchUtils;
import com.google.common.base.Preconditions;

import org.apache.commons.codec.binary.Base64OutputStream;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Methods to help managing document revision histories.</p>
 *
 * <p>This class is not complete, but should bring together a set of utility
 * methods for working with document trees.</p>
 */
public class RevisionHistoryHelper {

    // we are using json helper from mazha to it does not filter out
    // couchdb special fields
    private static JSONHelper sJsonHelper = new JSONHelper();

    /**
     * <p>Returns the list of revision IDs from a {@link DocumentRevs} object.
     * </p>
     *
     * <p>The list is (reverse) ordered, with the leaf revision ID for the
     * branch first.</p>
     *
     * <p>For the following input:</p>
     *
     * <pre>
     * {
     * ...
     * "_revisions": {
     *   "start": 4
     *   "ids": [
     *   "47d7102726fc89914431cb217ab7bace",
     *   "d8e1fb8127d8dd732d9ae46a6c38ae3c",
     *   "74e0572530e3b4cd4776616d2f591a96",
     *   "421ff3d58df47ea6c5e83ca65efb2fa9"
     *   ],
     * },
     * ...
     * }
     * </pre>
     *
     * <p>The returned value is:</p>
     *
     * <pre>
     * [
     *   "4-47d7102726fc89914431cb217ab7bace",
     *   "3-d8e1fb8127d8dd732d9ae46a6c38ae3c",
     *   "2-74e0572530e3b4cd4776616d2f591a96",
     *   "1-421ff3d58df47ea6c5e83ca65efb2fa9"
     * ]
     * </pre>
     *
     * @param documentRevs {@code DocumentRevs} object to process
     * @return an ordered list of revision IDs for the branch, leaf node first.
     *
     * @see com.cloudant.mazha.DocumentRevs
     */
    public static List<String> getRevisionPath(DocumentRevs documentRevs) {
        Preconditions.checkNotNull(documentRevs, "History must not be null");
        Preconditions.checkArgument(checkRevisionStart(documentRevs.getRevisions()),
                "Revision start must be bigger than revision ids' length");
        List<String> path = new ArrayList<String>();
        int start = documentRevs.getRevisions().getStart();
        for(String revId : documentRevs.getRevisions().getIds()) {
            path.add(start + "-" + revId);
            start --;
        }
        return path;
    }

    private static boolean checkRevisionStart(DocumentRevs.Revisions revisions) {
        return revisions.getStart() >= revisions.getIds().size();
    }

    /**
     * <p>Serialise a branch's revision history, in the form of a list of
     * {@link DocumentRevision}s, into the JSON format expected by CouchDB's _bulk_docs
     * endpoint.</p>
     *
     * @param history list of {@code DocumentRevision}s. This should be a complete list
     *                from the revision furthest down the branch to the root.
     * @return JSON-serialised {@code String} suitable for sending to CouchDB's
     *      _bulk_docs endpoint.
     *
     * @see com.cloudant.mazha.DocumentRevs
     */
    public static String revisionHistoryToJson(List<DocumentRevision> history, List<? extends Attachment> attachments) {
        Preconditions.checkNotNull(history, "History must not be null");
        Preconditions.checkArgument(history.size() > 0, "History must not have at least one DocumentRevision.");
        Preconditions.checkArgument(checkHistoryIsInDescendingOrder(history),
                "History must be in descending order.");

        DocumentRevision currentNode = history.get(0);
        Map<String, Object> m = currentNode.asMap();
        if (attachments != null && !attachments.isEmpty()) {
            // graft attachments on to m for this particular revision here
            addAttachments(attachments, m, CouchUtils.generationFromRevId(currentNode.getRevision()));
        }

        m.put(CouchConstants._revisions, createRevisions(history));

        return sJsonHelper.toJson(m);
    }

    private static void addAttachments(List<? extends Attachment> attachments, Map<String, Object> map, int revpos) {
        LinkedHashMap<String, Object> attsMap = new LinkedHashMap<String, Object>();
        map.put("_attachments", attsMap);
        for (Attachment att : attachments) {
            // we need to cast down to SavedAttachment, which we know is what the AttachmentManager gives us
            SavedAttachment savedAtt = (SavedAttachment)att;
            HashMap<String, Object> theAtt = new HashMap<String, Object>();
            // base64 encode this attachment
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Base64OutputStream bos = new Base64OutputStream(baos, true, 0, null);
            try {
                InputStream fis = savedAtt.getInputStream();
                int bufSiz = 1024;
                byte[] buf = new byte[bufSiz];
                int n = 0;
                do {
                    n = fis.read(buf);
                    if (n > 0) {
                        bos.write(buf, 0, n);
                    }
                } while (n > 0);
                // if the revpos of the current doc is higher than that of the attachment, it's a stub
                if (savedAtt.revpos < revpos) {
                    theAtt.put("stub", true);
                } else {
                    theAtt.put("data", baos.toString());  //base64 of data
                }
                theAtt.put("content_type", savedAtt.type);
                theAtt.put("revpos", savedAtt.revpos);
            } catch (FileNotFoundException fnfe) {
                continue;
            } catch (IOException ioe) {
                continue;
            }
            // now we are done, add the attachment to the hash
            attsMap.put(att.name, theAtt);
        }
    }

    private static Map<String, Object> createRevisions(List<DocumentRevision> history) {
        DocumentRevision currentNode = history.get(0);
        int start = CouchUtils.generationFromRevId(currentNode.getRevision());
        List<String> ids = new ArrayList<String>();
        for(DocumentRevision obj : history) {
            ids.add(CouchUtils.getRevisionIdSuffix(obj.getRevision()));
        }
        Map revisions = new HashMap<String, Object>();
        revisions.put(CouchConstants.start, start);
        revisions.put(CouchConstants.ids, ids);
        return revisions;
    }

    private static boolean checkHistoryIsInDescendingOrder(List<DocumentRevision> history) {
        for(int i = 0 ; i < history.size() - 1 ; i ++) {
            int l = CouchUtils.generationFromRevId(history.get(i).getRevision());
            int m = CouchUtils.generationFromRevId(history.get(i+1).getRevision());
            if(l <= m) {
                return false;
            }
        }
        return true;
    }
}
