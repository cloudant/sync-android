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

package com.cloudant.sync.sqlite;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * SQLDatabaseQuue provides the ability to ensure that the
 * only a single thread accesses the SQLDatabase. Tasks submitted to this
 * queue are guaranteed to be executed in the order they are received
 */
public class SQLDatabaseQueue {

    private final SQLDatabase db;
    private final ExecutorService queue = Executors.newSingleThreadExecutor();

    /**
     * Creates an SQLQueue for the database specified.
     * @param filename The file where the database is located
     * @throws IOException If an problem is encountered creating the DB
     */
    public SQLDatabaseQueue(String filename) throws IOException {
        this.db = SQLDatabaseFactory.createSQLDatabase(filename);
        queue.submit(new Runnable() {
            @Override
            public void run() {
                db.open();
            }
        });
    }

    /**
     * Updates the schema of the database.
     * @param schema The new Schmea for the database
     * @param version The version of the schema
     */
    public void updateSchema(final String[] schema, final int version){
        queue.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                SQLDatabaseFactory.updateSchema(db,schema,version);
                return null;
            }
        }); //fire and forget

    }

    /**
     * Submits a database task for execution
     * @param callable The task to be performed
     * @param <T> The type of object that is returned from the task
     * @return
     */
    public <T> Future<T> submit(SQLQueueCallable<T> callable){
        callable.setDb(db);
        callable.setRunInTransaction(false);
        return queue.submit(callable);

    }

    /**
     * submits a database task for execution in a transaction
     * @param callable
     * @param <T>
     * @return
     */
    public <T> Future<T> submitTransaction(SQLQueueCallable<T> callable){
        callable.setDb(db);
        callable.setRunInTransaction(true);
        return queue.submit(callable);
    }

    /**
     * Shuts down this database queue and closes
     * the underlying database connection. Any tasks
     * previously submitted for execution will still be
     * executed, however the queue will not accept additional
     * tasks
     */
    public void shutdown() {
        queue.submit(new Runnable() {
            @Override
            public void run() {
                db.close();
            }
        });
        queue.shutdown();
    }

    /**
     * Checks if @{link shutdown} has been called
     * @return true if @{link shutdown} has been called.
     */
    public boolean isShutdown() {
        return queue.isShutdown();
    }
}
