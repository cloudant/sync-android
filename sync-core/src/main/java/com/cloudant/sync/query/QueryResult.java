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
import com.cloudant.sync.datastore.DocumentRevision;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *  Iterable result of a query executed with {@link IndexManager}.
 *
 *  @see IndexManager
 */
public class QueryResult implements Iterable<DocumentRevision> {

    private final static int DEFAULT_BATCH_SIZE = 50;

    private final List<String> originalDocIds;
    private final Datastore datastore;
    private final List<String> fields;
    private final long skip;
    private final long limit;
    private final UnindexedMatcher matcher;

    public QueryResult(List<String> originalDocIds,
                       Datastore datastore,
                       List<String> fields,
                       long skip,
                       long limit,
                       UnindexedMatcher matcher) {
        this.originalDocIds = originalDocIds;
        this.datastore = datastore;
        this.fields = fields;
        this.skip = skip;
        this.limit = limit;
        this.matcher = matcher;
    }

    /**
     *  Returns the number of documents in this query result.
     *
     *  @return the number of documents {@code DocumentRevision} in this query result.
     */
    public int size() {
        return documentIds().size();
    }

    /**
     *  Returns a list of the document ids in this query result.
     *
     *  This method is implemented this way to ensure that the list of document ids is
     *  consistent with the iterator results.
     *
     *  @return list of the document ids
     */
    public List<String> documentIds() {
        List<String> documentIds = new ArrayList<String>();
        List<DocumentRevision> docs = Lists.newArrayList(iterator());
        for (DocumentRevision doc : docs) {
            documentIds.add(doc.getId());
        }
        return documentIds;
    }

    @Override
    public Iterator<DocumentRevision> iterator() {
        return new QueryResultIterator();
    }

    private class QueryResultIterator implements Iterator<DocumentRevision> {

        private Range range;
        private int nSkipped;
        private int nReturned;
        private boolean limitReached;
        private Iterator<DocumentRevision> documentBlock;

        private QueryResultIterator() {
            range = new Range(0, DEFAULT_BATCH_SIZE);
            nSkipped = 0;
            nReturned = 0;
            limitReached = false;
            documentBlock = populateDocumentBlock();
        }

        @Override
        public boolean hasNext() {
            return documentBlock.hasNext() ||
                   (!limitReached && range.location < originalDocIds.size());
        }

        @Override
        public DocumentRevision next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            if (!documentBlock.hasNext()) {
                documentBlock = populateDocumentBlock();
            }
            return documentBlock.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private Iterator<DocumentRevision> populateDocumentBlock() {
            List<DocumentRevision> docList = new ArrayList<DocumentRevision>();
            while (range.location < originalDocIds.size()) {
                range.length = Math.min(DEFAULT_BATCH_SIZE, originalDocIds.size() - range.location);
                List<String> batch = originalDocIds.subList(range.location,
                                                            range.location + range.length);
                List<BasicDocumentRevision> docs = datastore.getDocumentsWithIds(batch);
                for (DocumentRevision rev : docs) {
                    DocumentRevision innerRev;
                    innerRev = rev;  // Allows us to replace later if projecting

                    // Apply post-hoc matcher
                    if (matcher != null && !matcher.matches(innerRev)) {
                        continue;
                    }

                    // Apply skip (skip == 0 means disable)
                    if (skip > 0 && nSkipped < skip) {
                        nSkipped = nSkipped + 1;
                        continue;
                    }

                    // TODO - Add projection logic

                    docList.add(innerRev);

                    // Apply limit (limit == 0 means disable)
                    nReturned = nReturned + 1;
                    if (limit > 0 && nReturned >= limit) {
                        limitReached = true;
                        break;
                    }

                }

                range.location = range.location + range.length;

                if (limitReached) {
                    break;
                }

                if (!docList.isEmpty()) {
                    break;
                }
            }
            return docList.iterator();
        }
    }

    private class Range {
        public int location;
        public int length;

        private Range(int location, int length) {
            this.location = location;
            this.length = length;
        }
    }

}