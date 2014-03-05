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

package com.cloudant.sync.replication;

import com.cloudant.mazha.ChangesResult;
import com.cloudant.mazha.DocumentRevs;
import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.DocumentRevision;

import java.util.List;
import java.util.Map;
import java.util.Set;

interface CouchDB {

    public String getIdentifier();

    /**
     * Returns the name of the db
     */
    public String getDbName();

    /**
     * Returns true if database exists, otherwise false
     */
    public boolean exists();

    public Response create(Object object);
    public Response update(String id, Object object);
    public <T> T get(Class<T> classType, String id);
    public Response delete(String id, String rev);

    public String getCheckpoint(String checkpointId);
    public void putCheckpoint(String checkpointId, String sequence);

    public ChangesResult changes(String lastSequence, int limit);
    public ChangesResult changes(Replication.Filter filter,String lastSequence, int limit);
    public List<DocumentRevs> getRevisions(String documentId, String... revisionId);
    public void bulk(List<DocumentRevision> revisions);
    public void bulkSerializedDocs(List<String> serializedDocs);
    public Map<String, Set<String>> revsDiff(Map<String, Set<String>> revisions);
}
