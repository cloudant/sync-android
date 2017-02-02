/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.documentstore;


import java.util.List;
import java.util.Map;

/**
 * Created by tomblench on 22/01/16.
 */
public class ForceInsertItem {
    public InternalDocumentRevision rev;
    public List<String> revisionHistory;
    public Map<String, Object> attachments;
    public Map<String[], Map<String, PreparedAttachment>> preparedAttachments;
    public boolean pullAttachmentsInline;

    /**
     * @param rev                   A {@code DocumentRevision} containing the information for a
     *                              revision from a remote datastore.
     * @param revisionHistory       The history of the revision being inserted, including the rev
     *                              ID of {@code rev}. This list needs to be sorted in ascending
     *                              order.
     * @param attachments           Attachments metadata and inline data. Used when {@code
     *                              pullAttachmentsInline} true.
     * @param preparedAttachments   Attachments that have already been prepared, this is a Map of
     *                              String[docId,revId] → list of attachmentsNon-empty. Used when
     *                              {@code pullAttachmentsInline} false.
     * @param pullAttachmentsInline If true, use {@code attachments} metadata and data directly
     *                              from received JSON to add new attachments for this revision.
     *                              Else use {@code preparedAttachments} which were previously
     *                              downloaded and prepared by
     *                              {@link AttachmentManager#prepareAttachments(String, AttachmentStreamFactory, Map)}
     *                              as for example by
     *                              {@link com.cloudant.sync.internal.replication.PullStrategy}
     */
    public ForceInsertItem(InternalDocumentRevision rev, List<String> revisionHistory,
                           Map<String, Object> attachments, Map<String[],
            Map<String, PreparedAttachment>> preparedAttachments, boolean pullAttachmentsInline) {
        this.rev = rev;
        this.revisionHistory = revisionHistory;
        this.attachments = attachments;
        this.preparedAttachments = preparedAttachments;
        this.pullAttachmentsInline = pullAttachmentsInline;
    }
}
