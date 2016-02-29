/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.DocumentRevision;

public class DocumentModified {

    /**
     * Generic event for document create/update/delete
     * 
     * @param prevDocument
     *            Previous document revision (null for document create)
     * @param newDocument
     *            New document revision
     */

    public DocumentModified(DocumentRevision prevDocument,
                            DocumentRevision newDocument) {
        this.prevDocument = prevDocument;
        this.newDocument = newDocument;
    }

    public final DocumentRevision prevDocument;
    public final DocumentRevision newDocument;

}
