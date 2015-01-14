//  Copyright (c) 2014 Cloudant. All rights reserved.
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

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.Datastore;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 *  Iterable result of a query executed with {@link com.cloudant.sync.query.IndexManager}.
 *
 *  @see com.cloudant.sync.query.IndexManager
 */
public class QueryResult implements Iterable<BasicDocumentRevision> {

    private final static int DEFAULT_BATCH_SIZE = 50;

    private final List<String> docIds;
    private final Datastore datastore;
    private final List<String> fields;
    private final long skip;
    private final long limit;
    private final UnindexedMatcher matcher;

    public QueryResult(List<String> docIds,
                       Datastore datastore,
                       List<String> fields,
                       long skip,
                       long limit,
                       UnindexedMatcher matcher) {
        this.docIds = docIds;
        this.datastore = datastore;
        this.fields = fields;
        this.skip = skip;
        this.limit = limit;
        this.matcher = matcher;
    }

    /**
     *  Returns the number of documents in this query result.
     *
     *  @return the number of the {@code DocumentRevision} in this query result
     */
    public long size() {
        // TODO - skip, limit w.r.t. size

        return docIds.size();
    }

    /**
     *  Returns a list of the document ids in this query result.
     *
     *  @return list of the document ids
     */
    public List<String> documentIds() {
        // TODO - skip, limit w.r.t. document id list

        return docIds;
    }

    @Override
    public Iterator<BasicDocumentRevision> iterator() {

        /**
         * Partitions a set of document IDs into batches of DocumentRevision
         * objects, and provides an iterator over the whole, un-partitioned set
         * of revision objects (as if they were not batched).
         */
        return new Iterator<BasicDocumentRevision>() {

            // TODO - skip, limit, field projection, apply post-hoc matcher

            /** List containing lists of partitions document IDs */
            private final List<List<String>> subLists = this.partition(docIds, DEFAULT_BATCH_SIZE);
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
             * @see <a href="http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/
             * common/collect/Lists.html#partition(java.util.List, int)">Lists.partition</a>
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