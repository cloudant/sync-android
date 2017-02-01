/*
 * Copyright © 2014, 2017 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.query;

import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.internal.query.callables.SequenceNumberForIndexCallable;
import com.cloudant.sync.internal.query.callables.UpdateIndexCallable;
import com.cloudant.sync.internal.query.callables.UpdateMetadataForIndexCallable;
import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.QueryException;
import com.cloudant.sync.internal.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.internal.util.Misc;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Handles updating indexes for a given datastore.
 */
class IndexUpdater {

    private final Database database;

    private final SQLDatabaseQueue queue;

    private static final Logger logger = Logger.getLogger(IndexUpdater.class.getName());

    /**
     *  Constructs a new CDTQQueryExecutor using the indexes in 'database' to index documents from
     *  'datastore'.
     */
    public IndexUpdater(Database database, SQLDatabaseQueue queue) {
        this.database = database;
        this.queue = queue;
    }

    /**
     *  Update all indexes in a set.
     *
     *  These indexes are assumed to already exist.
     *
     *  @param indexes Map of indexes and their definitions.
     *  @param database The local datastore
     *  @param queue The executor service queue
     *  @return index update success status (true/false)
     */
    public static void updateAllIndexes(List<Index> indexes,
                                           Database database,
                                           SQLDatabaseQueue queue) throws QueryException {
        IndexUpdater updater = new IndexUpdater(database, queue);

        updater.updateAllIndexes(indexes);
    }

    /**
     *  Update a single index.
     *
     *  This index is assumed to already exist.
     *
     *  @param indexName Name of index to update
     *  @param fieldNames List of field names in the sort format
     *  @param database The local datastore
     *  @param queue The executor service queue
     *  @return index update success status (true/false)
     */
    public static void updateIndex(String indexName,
                                      List<FieldSort> fieldNames,
                                      Database database,
                                      SQLDatabaseQueue queue) throws QueryException {
        IndexUpdater updater = new IndexUpdater(database, queue);

        updater.updateIndex(indexName, fieldNames);
    }

    private void updateAllIndexes(List<Index> indexes) throws QueryException {

        for (Index index : indexes) {
            updateIndex(index.indexName, index.fieldNames);
        }
    }

    private void updateIndex(String indexName, List<FieldSort> fieldNames) throws QueryException {

        Misc.checkNotNullOrEmpty(indexName, "indexName");

        Changes changes;
        long lastSequence = sequenceNumberForIndex(indexName);
        try {
            do {
                changes = database.changes(lastSequence, 10000);
                updateIndex(indexName, fieldNames, changes, lastSequence);
                lastSequence = changes.getLastSequence();
            } while (changes.getResults().size() > 0);
        } catch (DocumentStoreException e) {
            String message = String.format(Locale.ENGLISH, "Failed to get changes feed from %d", lastSequence);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e.getCause());
        }
    }

    private void updateIndex(final String indexName,
                                final List<FieldSort> fieldNames,
                                final Changes changes,
                                long lastSequence) throws QueryException {

        Future<Void> result = queue.submitTransaction(new UpdateIndexCallable(changes, indexName,
                fieldNames));

        try {
            result.get();
        } catch (ExecutionException e) {
            String message = String.format("Execution error encountered whilst updating index %s", indexName);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        } catch (InterruptedException e) {
            String message = String.format("Execution interrupted error encountered whilst updating index %s", indexName);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        }

        // if there was a problem, we rolled back, and threw an exception, so the sequence won't be
        // updated. otherwise if we got here we can update the sequence.
        updateMetadataForIndex(indexName, lastSequence);


    }

    private long sequenceNumberForIndex(final String indexName) throws QueryException {
        Future<Long> sequenceNumber = queue.submit(new SequenceNumberForIndexCallable(indexName));

        long lastSequenceNumber = 0;
        try {
            lastSequenceNumber = sequenceNumber.get();
        } catch (ExecutionException e) {
            throw new QueryException("Execution error encountered:", e);
        } catch (InterruptedException e) {
            throw new QueryException("Execution interrupted error encountered:", e);
        }

        return lastSequenceNumber;
    }

    private void updateMetadataForIndex(final String indexName, final long lastSequence) throws QueryException {
        Future<Void> result = queue.submit(new UpdateMetadataForIndexCallable(lastSequence,
                indexName));

        try {
            result.get();
        } catch (ExecutionException e) {
            String message = String.format("Execution error encountered whilst updating index metadata for index %s", indexName);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        } catch (InterruptedException e) {
            String message = String.format("Execution interrupted error encountered whilst updating index metadata for index %s", indexName);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        }
    }

}
