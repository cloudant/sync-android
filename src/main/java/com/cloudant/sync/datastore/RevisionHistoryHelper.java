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
import com.cloudant.common.Log;
import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.util.CouchUtils;
import com.google.common.base.Preconditions;

import org.apache.commons.codec.binary.Base64OutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

    private final static String LOG_TAG = "RevisionHistoryHelper";

    // we are using json helper from mazha to it does not filter out
    // couchdb special fields

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
        for (String revId : documentRevs.getRevisions().getIds()) {
            path.add(start + "-" + revId);
            start--;
        }
        return path;
    }

    private static boolean checkRevisionStart(DocumentRevs.Revisions revisions) {
        return revisions.getStart() >= revisions.getIds().size();
    }

    /**
     * Serialise a branch's revision history, without attachments.
     * See {@link #revisionHistoryToJson(java.util.List, java.util.List)} for details.
     * @param history list of {@code DocumentRevision}s.
     * @return JSON-serialised {@code String} suitable for sending to CouchDB's
     *      _bulk_docs endpoint.
     */
    public static Map<String, Object> revisionHistoryToJson(List<DocumentRevision> history) {
        return revisionHistoryToJson(history, null);
    }

    /**
     * <p>Serialise a branch's revision history, in the form of a list of
     * {@link DocumentRevision}s, into the JSON format expected by CouchDB's _bulk_docs
     * endpoint.</p>
     *
     * @param history list of {@code DocumentRevision}s. This should be a complete list
     *                from the revision furthest down the branch to the root.
     * @param attachments list of {@code Attachment}s, if any. This allows the {@code _attachments}
     *                    dictionary to be correctly serialised. If there are no attachments, set
     *                    to null.
     * @return JSON-serialised {@code String} suitable for sending to CouchDB's
     *      _bulk_docs endpoint.
     *
     * @see com.cloudant.mazha.DocumentRevs
     */
    public static Map<String, Object> revisionHistoryToJson(List<DocumentRevision> history,
                                                            List<? extends Attachment> attachments) {
        Preconditions.checkNotNull(history, "History must not be null");
        Preconditions.checkArgument(history.size() > 0, "History must have at least one DocumentRevision.");
        Preconditions.checkArgument(checkHistoryIsInDescendingOrder(history),
                "History must be in descending order.");

        DocumentRevision currentNode = history.get(0);

        Map<String, Object> m = currentNode.asMap();
        if (attachments != null && !attachments.isEmpty()) {
            // graft attachments on to m for this particular revision here
            addAttachments(attachments, currentNode, m);
        }

        m.put(CouchConstants._revisions, createRevisions(history));

        return m;
    }

    /**
     * Create a MultipartAttachmentWriter object needed to subsequently write the JSON body and
     * attachments as a multipart/related stream
     */
    public static MultipartAttachmentWriter createMultipartWriter(List<DocumentRevision> history,
                                                                  List<? extends Attachment> attachments) {

        Preconditions.checkNotNull(history, "History must not be null");
        Preconditions.checkArgument(history.size() > 0, "History must have at least one DocumentRevision.");
        Preconditions.checkArgument(checkHistoryIsInDescendingOrder(history),
                "History must be in descending order.");

        DocumentRevision currentNode = history.get(0);

        MultipartAttachmentWriter mpw = null;
        int revpos = CouchUtils.generationFromRevId(currentNode.getRevision());
        for (Attachment att : attachments) {
            // we need to cast down to SavedAttachment, which we know is what the AttachmentManager gives us
            SavedAttachment savedAtt = (SavedAttachment) att;
            try {
                if (savedAtt.revpos < revpos) {
                    ; // skip
                } else {
                    if (!savedAtt.shouldInline()) {
                        // add
                        if (mpw == null) {
                            // 1st time init
                            mpw = new MultipartAttachmentWriter();
                            mpw.setBody(currentNode);
                        }
                        mpw.addAttachment(att);
                    } else {
                        // skip
                    }
                }
            } catch (IOException ioe) {
                Log.w(LOG_TAG, "IOException caught when adding multiparts: "+ioe);
            }
        }
        if (mpw != null) {
            mpw.close();
        }
        return mpw;
    }

    /**
     * Add attachment entries to the _attachments dictionary of the revision
     * If the attachment should be inlined, then insert the attachment data as a base64 string
     * If it isn't inlined, set follows=true to show it will be included in the multipart/related
     */
    private static void addAttachments(List<? extends Attachment> attachments,
                                   DocumentRevision revision,
                                   Map<String, Object> outMap) {
        LinkedHashMap<String, Object> attsMap = new LinkedHashMap<String, Object>();
        int revpos = CouchUtils.generationFromRevId(revision.getRevision());
        outMap.put("_attachments", attsMap);
        for (Attachment att : attachments) {
            // we need to cast down to SavedAttachment, which we know is what the AttachmentManager gives us
            SavedAttachment savedAtt = (SavedAttachment)att;
            HashMap<String, Object> theAtt = new HashMap<String, Object>();
            try {
                if (savedAtt.revpos < revpos) {
                    // if the revpos of the current doc is higher than that of the attachment, it's a stub
                    theAtt.put("stub", true);
                } else {
                    if (!savedAtt.shouldInline()) {
                        theAtt.put("follows", true);
                    } else {
                        theAtt.put("follows", false);
                        // base64 encode this attachment
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        Base64OutputStream bos = new Base64OutputStream(baos, true, 0, null);
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
                        theAtt.put("data", baos.toString());  //base64 of data
                    }
                    theAtt.put("length", savedAtt.getSize());
                    theAtt.put("content_type", savedAtt.type);
                    theAtt.put("revpos", savedAtt.revpos);
                }
            } catch (IOException ioe) {
                // if we can't read the file containing the attachment then skip it
                // (this should only occur if someone tampered with the attachments directory
                // or something went seriously wrong)
                Log.w(LOG_TAG, "Caught IOException in addAttachments whilst reading attachment " + att + ", skipping it: " + ioe);
                continue;
            }
            // now we are done, add the attachment to the map
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
