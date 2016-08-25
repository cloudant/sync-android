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
 *
 * @api_private
 */
class SQLQueueCallable<T> implements Callable<T> {

    private SQLDatabase db;
    private boolean runInTransaction;
    private SQLCallable<T> sqlCallable;

    /**
     * <p>
     *     Create a SQLQueueCallable object which will invoke the SQLCallable on the SQLDatabase
     * </p>
     * <p>
     *     The SQLCallable will not be invoked in a new transaction
     * </p>
     * @param db The SQLDatabase to use for invoking
     * @param sqlCallable The SQLCallable to invoke
     */
    SQLQueueCallable(SQLDatabase db, SQLCallable<T> sqlCallable) {
        this.db = db;
        this.sqlCallable = sqlCallable;
    }

    /**
     * <p>
     *     Create a SQLQueueCallable object which will invoke the SQLCallable on the SQLDatabase
     * </p>
     * <p>
     *     The SQLCallable will be invoked in a new transaction if runInTransaction is true
     * </p>
     * @param db The SQLDatabase to use for invoking
     * @param sqlCallable The SQLCallable to invoke
     * @param runInTransaction Whether to invoke the SQLCallable in a new transaction
     */
    SQLQueueCallable(SQLDatabase db, SQLCallable<T> sqlCallable, boolean runInTransaction) {
        this.db = db;
        this.runInTransaction = runInTransaction;
        this.sqlCallable = sqlCallable;
    }

    @Override
    final public T call() throws Exception {
        if(runInTransaction){
            try {
                db.beginTransaction();
                //call(db) throws an exception if the transaction should be rolled back
                T returned = sqlCallable.call(db);
                db.setTransactionSuccessful();
                return returned;
            } finally {
                db.endTransaction();
            }
        } else {
            return sqlCallable.call(db);
        }
    }

}
