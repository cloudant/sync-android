//  Copyright (c) 2015 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.datastore;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  A document revision that has been projected.
 *
 *  This class implements a version of mutableCopy which returns the full
 *  document when called, to prevent accidental data loss which might come
 *  from saving a projected document.
 */
public class ProjectedDocumentRevision extends BasicDocumentRevision {

    private static final Logger logger = Logger.getLogger(ProjectedDocumentRevision.class.getCanonicalName());

    Datastore datastore;

    ProjectedDocumentRevision(String docId,
                              String revId,
                              DocumentBody body,
                              BasicDocumentRevisionOptions options,
                              Datastore datastore) {
        super(docId, revId, body, options);
        this.datastore = datastore;
    }

    @Override
    public MutableDocumentRevision mutableCopy() {
        try {
            BasicDocumentRevision rev = datastore.getDocument(this.getId());

            // Don't want to return an updated version, breaks contract of mutableCopy
            if (!rev.getRevision().equals(this.getRevision())) {
                return null;
            }

            return rev.mutableCopy();
        } catch (DocumentNotFoundException e){
            logger.log(Level.SEVERE,String.format("Failed to load document %s from datastore",this.toString()),e);
            return null;
        }
    }
}
