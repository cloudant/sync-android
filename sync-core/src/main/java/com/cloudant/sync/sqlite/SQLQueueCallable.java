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

import java.util.concurrent.Callable;

/**
 * A task which performs actions on a SQLDatabase passed in when
 * called.
 */
public abstract class SQLQueueCallable<T> implements Callable<T> {

    private SQLDatabase db;
    private boolean runInTransaction = false;

    @Override
    final public T call() throws Exception {
        if(runInTransaction){
            try {
                db.beginTransaction();
                //call(db) throws an exception if the transaction should be rolled back
                T returned =  call(db);
                db.setTransactionSuccessful();
                return returned;
            } finally {
                db.endTransaction();
            }
        } else {
            return call(db);
        }
    }

    /**
     * Called either within a transaction or not depending on if
     * @{link com.cloudant.sync.sqlite.SQLDatabaseQueue#setRunInTransaction(SQLQueueCallable} or
     * {@link com.cloudant.sync.sqlite.SQLDatabaseQueue#submit(SQLQueueCallable)} is called
     *
     * When called within a transaction, to mark the transaction as successful
     * simply return from this method, to cause a rollback throw an exception
     * @param db The SQLDatabase for this transaction
     * @return The tasks result
     * @throws Exception If an error occurred and a rollback is needed
     */
    public abstract T call(SQLDatabase db) throws Exception;

    /**
     * Sets the database for this task
     * @param db The SQLDatabase for this task
     */
    final void setDb(SQLDatabase db){
        this.db = db;
    }

    /**
     * Sets if this callable should happen within a transaction
     * @param runInTransaction whether this should be called in a transaction
     */
     final void setRunInTransaction(boolean runInTransaction) {
        this.runInTransaction = runInTransaction;
    }
}
