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

package com.cloudant.sync.indexing;

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class BasicQueryResult implements QueryResult {

    private final static int DEFAULT_BATCH_SIZE = 50;
    private final List<String> documentIds;
    private final Datastore datastore;
    private final int batchSize;

    public BasicQueryResult(List<String> documentIds, Datastore datastore) {
        this(documentIds, datastore, DEFAULT_BATCH_SIZE);
    }

    public BasicQueryResult(List<String> documentIds, Datastore datastore, int batchSize) {
        this.documentIds = documentIds;
        this.datastore = datastore;
        this.batchSize = batchSize;
    }

    @Override
    public long size() {
        return documentIds.size();
    }

    @Override
    public List<String> documentIds() {
        return documentIds;
    }

    @Override
    public Iterator<BasicDocumentRevision> iterator() {

        /**
         * Partitions a set of document IDs into batches of DocumentRevision
         * objects, and provides an iterator over the whole, un-partitioned set
         * of revision objects (as if they were not batched).
         */
        return new Iterator<BasicDocumentRevision>() {

            /** List containing lists of partitions document IDs */
            private final List<List<String>> subLists = this.partition(documentIds, batchSize);
            /** The current partition's iterator of document objects */
            private Iterator<BasicDocumentRevision> subIterator = null;

            @Override
            public boolean hasNext() {
                if(subIterator == null) {
                    return subLists.size() > 0;
                } else {
                    return this.subIterator.hasNext() || subLists.size() > 0;
                }
            }

            @Override
            public BasicDocumentRevision next() {
                if(subIterator == null || !subIterator.hasNext()) {
                    List<String> ids = subLists.remove(0);
                    subIterator = this.nextSubIterator(ids);
                }
                
                return subIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            /**
             * Partition a list of document IDs into batches of batchSize.
             *
             * Return a mutable list of consecutive sublists.
             * Same as Guava's "Lists.partition" except the result list is mutable.
             * It is needed because this iterator removes sublist from the partitions
             * as it goes.
             *
             * @See http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/Lists.html#partition(java.util.List, int)
             */
            private List<List<String>> partition(List<String> documentIds, int batchSize) {
                List<List<String>> partitions = Lists.partition(documentIds, batchSize);
                List<List<String>> res = new LinkedList<List<String>>();
                for(List<String> p : partitions) {
                    res.add(p);
                }
                return res;
            }

            /**
             * Load the next partition of DocumentRevision objects for the
             * iterator.
             *
             * @param ids the IDs of the revisions to load.
             * @return an iterator over the DocumentRevision objects for `ids`.
             */
            private Iterator<BasicDocumentRevision> nextSubIterator(List<String> ids) {
                HashMap<String, BasicDocumentRevision> map = new HashMap<String, BasicDocumentRevision>();
                for(BasicDocumentRevision revision : datastore.getDocumentsWithIds(ids)) {
                    map.put(revision.getId(), revision);
                }
                List<BasicDocumentRevision> revisions = new ArrayList<BasicDocumentRevision>(ids.size());
                // return list of DocumentRevision that in the same order as input "ids"
                for(String id : ids) {
                    BasicDocumentRevision revision = map.get(id);
                    if(revision != null ) {
                        revisions.add(revision);
                    }
                }
                return revisions.iterator();
            }
        };
    }
}
