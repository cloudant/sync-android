/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.internal.mazha.ChangesResult;
import com.cloudant.sync.internal.mazha.CouchClient;
import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.mazha.Response;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.documentstore.DocumentRevsList;
import com.cloudant.sync.internal.documentstore.MultipartAttachmentWriter;
import com.cloudant.sync.internal.documentstore.UnsavedStreamAttachment;
import com.cloudant.sync.replication.PullFilter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

interface CouchDB {

    String getIdentifier();

    /**
     * @return true if database exists, otherwise false
     */
    boolean exists();

    Response create(Object object);
    Response update(String id, Object object);
    <T> T get(Class<T> classType, String id);
    Response delete(String id, String rev);

    String getCheckpoint(String checkpointId);
    void putCheckpoint(String checkpointId, String sequence);

    ChangesResult changes(Object lastSequence, int limit);
    ChangesResult changes(PullFilter filter, Object lastSequence, int limit);
    List<DocumentRevs> getRevisions(String documentId,
                                           Collection<String> revisionIds,
                                           Collection<String> attsSince,
                                           boolean pullAttachmentsInline);
    void bulkCreateDocs(List<InternalDocumentRevision> revisions);
    void bulkCreateSerializedDocs(List<String> serializedDocs);
    List<Response> putMultiparts(List<MultipartAttachmentWriter> multiparts);
    Map<String, CouchClient.MissingRevisions> revsDiff(Map<String, Set<String>> revisions);
    UnsavedStreamAttachment getAttachmentStream(String id, String rev, String attachmentName, String contentType, String encoding);

    Iterable<DocumentRevsList> bulkGetRevisions(List<BulkGetRequest> requests,
                                                   boolean pullAttachmentsInline);
    boolean isBulkSupported();

}
