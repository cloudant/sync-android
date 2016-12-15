/*
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.event.notifications;

import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentRevision;

/**
 * <p>
 * Event for document delete
 * </p>
 *
 * <p>This event is posted by
 * {@link Database#delete(DocumentRevision)} and {@link Database#delete(String)}.
 * </p>
 *
 * @api_public
 */
public class DocumentDeleted extends DocumentModified {

    /**
     * Event for document delete
     *
     * @param prevDocument
     *            Previous document revision
     * @param newDocument
     *            New (empty) document revision
     * 
     */
    public DocumentDeleted(DocumentRevision prevDocument,
                           DocumentRevision newDocument) {
        super(prevDocument, newDocument);
    }

}
