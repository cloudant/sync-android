/*
 * Copyright (c) 2015 IBM Corp. All rights reserved.
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

package com.cloudant.sync.documentstore;

/**
 * A local Document, this document does not have a history, or the concept of revisions
 *
 * @api_private
 */
public class LocalDocument {

    /**
     * The ID of the local document
     */
    public final String docId;
    /**
     * The body of the local document
     */
    public final DocumentBody body;

    /**
     * Creates a local document
     * @param docId The documentId for this document
     * @param body The body of the local document
     */
    public LocalDocument(String docId, DocumentBody body){
        this.docId = docId;
        this.body = body;
    }


}
