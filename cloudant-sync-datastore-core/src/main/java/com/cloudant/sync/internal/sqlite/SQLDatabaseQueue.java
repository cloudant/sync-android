/*
 * Copyright © 2015, 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.sqlite;

import com.cloudant.sync.documentstore.encryption.KeyProvider;
import com.cloudant.sync.documentstore.encryption.NullKeyProvider;
import com.cloudant.sync.internal.documentstore.migrations.Migration;
import com.sun.xml.internal.ws.util.CompletedFuture;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SQLDatabaseQuue provides the ability to ensure that the
 * only a single thread accesses the SQLDatabase. Tasks submitted to this
 * queue are guaranteed to be executed in the order they are received
 *
 * @api_private
 */
public class SQLDatabaseQueue {

    private final SQLDatabase db;
    private final ExecutorService queue;
    private final Logger logger = Logger.getLogger(SQLDatabase.class.getCanonicalName());
    private AtomicBoolean acceptTasks = new AtomicBoolean(true);
    private String sqliteVersion = null;
    /**
     * Creates an SQLQueue for the database specified.
     * @param file The file where the database is located
     * @throws IOException If an problem is encountered creating the DB
     * @throws SQLException If the database cannot be opened.
     */
    public SQLDatabaseQueue(File file) throws IOException, SQLException {
        this(file, new NullKeyProvider());
    }

    /**
     * Creates an SQLQueue for the SQLCipher-based database specified.
     * @param file The file where the database is located
     * @param provider The key provider object that contains the user-defined SQLCipher key.
     *                 Supply a NullKeyProvider to use a non-encrypted database.
     * @throws IOException If a problem occurs creating the database
     * @throws SQLException If the database cannot be opened.
     */
    public SQLDatabaseQueue(final File file, KeyProvider provider) throws IOException, SQLException {
        queue = Executors.newSingleThreadExecutor(new ThreadFactory(file));
        this.db = SQLDatabaseFactory.openSQLDatabase(file, provider);
        queue.execute(new Runnable() {
            @Override
            public void run() {
                db.open();
            }
        });
    }

    /**
     * Updates the schema of the database.
     * @param migration Object which performs migration; should not check or set version
     * @param version The version of the schema
     */
    public void updateSchema(final Migration migration, final int version){
        queue.execute(new UpdateSchemaCallable(migration, version)); // Fire and forget
    }

    /**
     * Returns the current version of the database.
     * @return The current version of the database.
     * @throws SQLException Throws if there was an error getting the current database version
     */
    public int getVersion() throws SQLException {
        try {
            return this.submit(new VersionCallable()).get();
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
    public <T> Future<T> submit(SQLCallable<T> callable){
        try {
            T result = new SQLQueueCallable<T>(db, callable).call();
            return new CompletedFuture<T>(result, null);
        } catch (Exception e) {
            return new CompletedFuture<T>(null, e);
        }
//        return this.submitTaskToQueue(new SQLQueueCallable<T>(db, callable));
    }

    /**
     * Submits a database task for execution in a transaction
     * @param callable The task to be performed
     * @param <T> The type of object that is returned from the task
     * @throws RejectedExecutionException thrown when the queue has been shutdown
     * @return Future representing the task to be executed.
     */
    public <T> Future<T> submitTransaction(SQLCallable<T> callable){
        try {
            T result = new SQLQueueCallable<T>(db, callable, true).call();
            return new CompletedFuture<T>(result, null);
        } catch (Exception e) {
            return new CompletedFuture<T>(null, e);
        }
    }

    /**
     * Shuts down this database queue and closes
     * the underlying database connection. Any tasks
     * previously submitted for execution will still be
     * executed, however the queue will not accept additional
     * tasks
     */
    public void shutdown() {
        // If shutdown has already been called then we don't need to shutdown again
        if (acceptTasks.getAndSet(false)) {
            //pass straight to queue, tasks passed via submitTaskToQueue will now be blocked.
            Future<?> close = queue.submit(new Runnable() {
                @Override
                public void run() {
                    db.close();
                }
            });
            queue.shutdown();
            try {
                close.get();
                queue.awaitTermination(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "Interrupted while waiting for queue to terminate", e);
            } catch (ExecutionException e) {
                logger.log(Level.SEVERE, "Failed to close database", e);
            }
        } else {
            logger.log(Level.WARNING, "Database is already closed.");
        }
    }

    /**
     * Checks if {@link SQLDatabaseQueue#shutdown()} has been called
     * @return true if {@link SQLDatabaseQueue#shutdown()} has been called.
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
        if(acceptTasks.get()){
            return queue.submit(callable);
        } else {
            throw new RejectedExecutionException("Database is closed");
        }
    }

    /**
     * Returns the SQLite Version.
     * @return The SQLite version or "Unknown" if the version could not be determined.
     */
    public synchronized String getSQLiteVersion() {

        if (this.sqliteVersion == null) {
            try {
                this.sqliteVersion = this.submit(new SQLiteVersionCallable()).get();
                return sqliteVersion;
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Could not determine SQLite version", e);
            } catch (ExecutionException e) {
                logger.log(Level.WARNING, "Could not determine SQLite version", e);
            }
            this.sqliteVersion = "unknown";
        }

        return this.sqliteVersion;


    }

    private static class SQLiteVersionCallable implements SQLCallable<String> {
        @Override
        public String call(SQLDatabase db) throws Exception {
            Cursor cursor = db.rawQuery("SELECT sqlite_version()", null);

            StringBuilder stringBuilder = new StringBuilder();
            while (cursor.moveToNext()) {
                stringBuilder.append(cursor.getString(0));
            }
            return stringBuilder.toString();
        }
    }

    private static class VersionCallable implements SQLCallable<Integer> {
        @Override
        public Integer call(SQLDatabase db) throws Exception {
            return db.getVersion();
        }
    }

    private static class ThreadFactory implements java.util.concurrent.ThreadFactory {
        private final File file;

        public ThreadFactory(File file) {
            this.file = file;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "SQLDatabaseQueue - "+ file);
        }
    }

    private class UpdateSchemaCallable implements Runnable {
        private final Migration migration;
        private final int version;

        public UpdateSchemaCallable(Migration migration, int version) {
            this.migration = migration;
            this.version = version;
        }

        @Override
        public void run() {
            try {
                SQLDatabaseFactory.updateSchema(db, migration, version);
            } catch (SQLException e){
                logger.log(Level.SEVERE, "Failed to update database schema",e);
            }
        }
    }
}
