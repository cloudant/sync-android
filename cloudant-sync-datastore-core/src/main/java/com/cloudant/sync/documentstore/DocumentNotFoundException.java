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

package com.cloudant.sync.documentstore;

/**
 * Thrown when a document cannot be found.
 *
 * @api_public
 */
public class DocumentNotFoundException extends DocumentException {

    public DocumentNotFoundException(String message) {
        super(message);
    }

    public DocumentNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DocumentNotFoundException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a document not found exception with the default message
     *
     * @param docId The document id of the document that could not be found
     * @param revId The rev id of the document that could not be found
     */
    public DocumentNotFoundException(String docId, String revId) {
        super(createMessage(docId, revId));
    }

    public DocumentNotFoundException(String docId, String revId, Throwable e) {
        super(createMessage(docId, revId), e);
    }

    private static String createMessage(String docId, String revId) {
        if (revId != null) {
            return String.format("Could not find document with id %s at revision %s", docId, revId);
        } else {
            return String.format("Could not find document with id %s", docId);
        }
    }

}