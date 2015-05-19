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

import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SQLDatabaseQuue provides the ability to ensure that the
 * only a single thread accesses the SQLDatabase. Tasks submitted to this
 * queue are guaranteed to be executed in the order they are received
 */
public class SQLDatabaseQueue {

    private final SQLDatabase db;
    private final ExecutorService queue = Executors.newSingleThreadExecutor();
    private final Logger logger = Logger.getLogger(SQLDatabase.class.getCanonicalName());
    private volatile boolean acceptTasks = true;

    /**
     * Creates an SQLQueue for the database specified.
     * @param filename The file where the database is located
     * @throws IOException If an problem is encountered creating the DB
     */
    public SQLDatabaseQueue(String filename) throws IOException {
        this(filename, new NullKeyProvider());
    }

    /**
     * Creates an SQLQueue for the SQLCipher-based database specified.
     * @param filename The file where the database is located
     * @param provider The key provider object that contains the user-defined SQLCipher key.
     *                 Supply a NullKeyProvider to use a non-encrypted database.
     * @throws IOException If a problem occurs creating the database
     */
    public SQLDatabaseQueue(String filename, KeyProvider provider) throws IOException {
        this.db = SQLDatabaseFactory.createSQLDatabase(filename, provider);
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
        this.submit(new SQLQueueCallable<Object>() {
            @Override
            public Object call(SQLDatabase db) throws Exception {
                SQLDatabaseFactory.updateSchema(db, schema, version);
                return null;
            }
        }); //fire and forget
    }

    /**
     * Returns the current version of the database.
     * @return The current version of the database.
     * @throws SQLException Throws if there was an error getting the current database version
     */
    public int getVersion() throws SQLException {
        try {
            return this.submit(new SQLQueueCallable<Integer>() {
                @Override
                public Integer call(SQLDatabase db) throws Exception {
                    return db.getVersion();
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get database version", e);
            throw new SQLException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get database version", e);
            throw new SQLException(e);
        }
    }

    /**
     * Submits a database task for execution
     * @param callable The task to be performed
     * @param <T> The type of object that is returned from the task
     * @throws RejectedExecutionException Thrown when the queue has been shutdown
     * @return Future representing the task to be executed.
     */
    public <T> Future<T> submit(SQLQueueCallable<T> callable){
        callable.setDb(db);
        callable.setRunInTransaction(false);
        return this.submitTaskToQueue(callable);
    }

    /**
     * Submits a database task for execution in a transaction
     * @param callable The task to be performed
     * @param <T> The type of object that is returned from the task
     * @throws RejectedExecutionException thrown when the queue has been shutdown
     * @return Future representing the task to be executed.
     */
    public <T> Future<T> submitTransaction(SQLQueueCallable<T> callable){
        callable.setDb(db);
        callable.setRunInTransaction(true);
        return this.submitTaskToQueue(callable);
    }

    /**
     * Shuts down this database queue and closes
     * the underlying database connection. Any tasks
     * previously submitted for execution will still be
     * executed, however the queue will not accept additional
     * tasks
     */
    public void shutdown() {
        acceptTasks = false;
        //pass straight to queue, tasks passed via submitTaskToQueue will now be blocked.
        queue.submit(new Runnable() {
            @Override
            public void run() {
                db.close();
            }
        });
        queue.shutdown();
        try {
            queue.awaitTermination(5,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Interrupted while waiting for queue to terminate",e);
        }

    }

    /**
     * Checks if @{link shutdown} has been called
     * @return true if @{link shutdown} has been called.
     */
    public boolean isShutdown() {
        return queue.isShutdown();
    }

    /**
     * Adds a task to the queue, checking if the queue is still open
     * to accepting tasks
     * @param callable The task to submit to the queue
     * @param <T> The type of object that the callable returns
     * @return Future representing the task to be executed.
     * @throws RejectedExecutionException If the queue has been shutdown.
     */
    private <T> Future<T> submitTaskToQueue(SQLQueueCallable<T> callable){
        if(acceptTasks){
            return queue.submit(callable);
        } else {
            throw new RejectedExecutionException("Database is closed");
        }
    }
}
