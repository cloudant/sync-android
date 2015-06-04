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

import com.cloudant.android.Base64OutputStreamFactory;
import com.cloudant.common.CouchConstants;
import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.replication.PushAttachmentsInline;
import com.cloudant.sync.util.CouchUtils;
import com.google.common.base.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>Methods to help managing document revision histories.</p>
 *
 * <p>This class is not complete, but should bring together a set of utility
 * methods for working with document trees.</p>
 */
public class RevisionHistoryHelper {

    private final static Logger logger = Logger.getLogger(RevisionHistoryHelper.class
            .getCanonicalName());

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
     * See {@link #revisionHistoryToJson(java.util.List, java.util.List, com.cloudant.sync.replication.PushAttachmentsInline, int)} for details.
     * @param history list of {@code DocumentRevision}s.
     * @return JSON-serialised {@code String} suitable for sending to CouchDB's
     *      _bulk_docs endpoint.
     */
    public static Map<String, Object> revisionHistoryToJson(List<BasicDocumentRevision> history) {
        return revisionHistoryToJson(history, null, null, 0);
    }

    /**
     * <p>Serialise a branch's revision history, in the form of a list of
     * {@link BasicDocumentRevision}s, into the JSON format expected by CouchDB's _bulk_docs
     * endpoint.</p>
     *
     * @param history list of {@code DocumentRevision}s. This should be a complete list
     *                from the revision furthest down the branch to the root.
     * @param attachments list of {@code Attachment}s, if any. This allows the {@code _attachments}
     *                    dictionary to be correctly serialised. If there are no attachments, set
     *                    to null.
     * @param inlinePreference strategy to decide whether to upload attachments inline or separately.
     * @param minRevPos generation number of most recent ancestor on the remote database. If the
     *                  {@code revpos} value of a given attachment is greater than {@code minRevPos},
     *                  then it is newer than the version on the remote database and must be sent.
     *                  Otherwise, a stub can be sent.
     * @return JSON-serialised {@code String} suitable for sending to CouchDB's
     *      _bulk_docs endpoint.
     *
     * @see com.cloudant.mazha.DocumentRevs
     */
    public static Map<String, Object> revisionHistoryToJson(List<BasicDocumentRevision> history,
                                                            List<? extends Attachment> attachments,
                                                            PushAttachmentsInline inlinePreference,
                                                            int minRevPos) {
        Preconditions.checkNotNull(history, "History must not be null");
        Preconditions.checkArgument(history.size() > 0, "History must have at least one DocumentRevision.");
        Preconditions.checkArgument(checkHistoryIsInDescendingOrder(history),
                "History must be in descending order.");

        BasicDocumentRevision currentNode = history.get(0);

        Map<String, Object> m = currentNode.asMap();
        if (attachments != null && !attachments.isEmpty()) {
            // graft attachments on to m for this particular revision here
            addAttachments(attachments, m, inlinePreference, minRevPos);
        }

        m.put(CouchConstants._revisions, createRevisions(history));

        return m;
    }

    /**
     * Create a {@code MultipartAttachmentWriter} object needed to subsequently write the JSON body and
     * attachments as a MIME multipart/related stream
     *
     * @param revision document revision. This allows the JSON to be serialised into the first MIME
     *                 body part
     * @param attachments list of {@code Attachment}s, if any. This allows the {@code _attachments}
     *                    dictionary to be serialised into subsequent MIME body parts
     * @param inlinePreference strategy to decide whether to upload attachments inline or separately.
     * @param minRevPos generation number of most recent ancestor on the remote database. If the
     *                  {@code revpos} value of a given attachment is greater than {@code minRevPos},
     *                  then it is newer than the version on the remote database and must be sent.
     *                  Otherwise, a stub can be sent.
     *
     * @return a {@code MultipartAttachmentWriter} object, or {@code null} if there are no attachments
     *         or all attachments are to be sent inline
     *
     * @see com.cloudant.sync.datastore.MultipartAttachmentWriter
     */
    public static MultipartAttachmentWriter createMultipartWriter(BasicDocumentRevision revision,
                                                                  List<? extends Attachment> attachments,
                                                                  PushAttachmentsInline inlinePreference,
                                                                  int minRevPos) {
        MultipartAttachmentWriter mpw = null;
        for (Attachment att : attachments) {
            // we need to cast down to SavedAttachment, which we know is what the AttachmentManager gives us
            SavedAttachment savedAtt = (SavedAttachment) att;
            try {
                // add the attachment if it's newer than the minimum revpos and it's not being sent inline
                if (savedAtt.revpos > minRevPos && !savedAtt.shouldInline(inlinePreference)) {
                    // add
                    if (mpw == null) {
                        // 1st time init
                        mpw = new MultipartAttachmentWriter();
                        mpw.setBody(revision);
                    }
                    mpw.addAttachment(savedAtt,
                            savedAtt.encoding == Attachment.Encoding.Plain ? savedAtt.length : savedAtt.encodedLength);
                }
            } catch (IOException ioe) {
                logger.log(Level.WARNING,"IOException caught when adding multiparts",ioe);
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
                                   Map<String, Object> outMap,
                                   PushAttachmentsInline inlinePreference,
                                   int minRevPos) {
        LinkedHashMap<String, Object> attsMap = new LinkedHashMap<String, Object>();
        outMap.put("_attachments", attsMap);
        for (Attachment att : attachments) {
            // we need to cast down to SavedAttachment, which we know is what the AttachmentManager gives us
            SavedAttachment savedAtt = (SavedAttachment)att;
            HashMap<String, Object> theAtt = new HashMap<String, Object>();
            try {
                // add the attachment if it's newer than the minimum revpos
                if (savedAtt.revpos > minRevPos) {
                    if (!savedAtt.shouldInline(inlinePreference)) {
                        theAtt.put("follows", true);
                    } else {
                        theAtt.put("follows", false);
                        // base64 encode this attachment
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        OutputStream bos = Base64OutputStreamFactory.get(baos);
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
                        // out of paranoia, close and flush - docs don't say what you have to do to
                        // ensure all base64 is written, but close seems to do the right thing with
                        // the apache codec implementation
                        bos.flush();
                        bos.close();
                        theAtt.put("data", baos.toString());  //base64 of data
                    }
                    theAtt.put("length", savedAtt.length);
                    theAtt.put("encoded_length", savedAtt.encodedLength);
                    theAtt.put("content_type", savedAtt.type);
                    theAtt.put("revpos", savedAtt.revpos);
                    if (savedAtt.encoding != Attachment.Encoding.Plain) {
                        theAtt.put("encoding", savedAtt.encoding.toString().toLowerCase());
                    }
                } else {
                    // if the revpos of the attachment is higher than the minimum, it's a stub
                    theAtt.put("stub", true);
                }
            } catch (IOException ioe) {
                // if we can't read the file containing the attachment then skip it
                // (this should only occur if someone tampered with the attachments directory
                // or something went seriously wrong)
                logger.log(Level.WARNING,String.format("Error reading attachment %s",att),ioe);
                continue;
            }
            // now we are done, add the attachment to the map
            attsMap.put(att.name, theAtt);
        }
    }

    private static Map<String, Object> createRevisions(List<BasicDocumentRevision> history) {
        BasicDocumentRevision currentNode = history.get(0);
        int start = CouchUtils.generationFromRevId(currentNode.getRevision());
        List<String> ids = new ArrayList<String>();
        for(BasicDocumentRevision obj : history) {
            ids.add(CouchUtils.getRevisionIdSuffix(obj.getRevision()));
        }
        Map revisions = new HashMap<String, Object>();
        revisions.put(CouchConstants.start, start);
        revisions.put(CouchConstants.ids, ids);
        return revisions;
    }

    private static boolean checkHistoryIsInDescendingOrder(List<BasicDocumentRevision> history) {
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
