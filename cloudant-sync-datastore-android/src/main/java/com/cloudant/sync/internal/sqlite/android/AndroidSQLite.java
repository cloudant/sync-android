/*
 * Copyright © 2013, 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.sqlite.android;

import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteDatabase;

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.Misc;

import java.io.File;
import java.sql.SQLException;

/**
 * @api_private
 */
public class AndroidSQLite extends SQLDatabase {

    android.database.sqlite.SQLiteDatabase database = null;

    public static AndroidSQLite open(File path) {
        SQLiteDatabase db;
        if (path != null) {
            db = SQLiteDatabase.openOrCreateDatabase(path, null);
        } else {
            db = SQLiteDatabase.create(null);
        }
        return new AndroidSQLite(db);
    }

    public AndroidSQLite(final android.database.sqlite.SQLiteDatabase database) {
        this.database = database;

    }

    @Override
    public void compactDatabase() {
        database.execSQL("VACUUM");
    }

    @Override
    public void open() {
        // database should be already opened
    }

    @Override
    public void close() {
        this.database.close();
    }

    // This implementation of isOpen will only return true if the database is open AND the current
    // thread has not called the close() method on this object previously, this makes it compatible
    // with the JavaSE SQLiteWrapper, where each thread closes its own connection.
    @Override
    public boolean isOpen() {
        return this.database.isOpen();
    }

    @Override
    public void beginTransaction() {
        this.database.beginTransaction();
    }

    @Override
    public void endTransaction() {
        this.database.endTransaction();
    }

    @Override
    public void setTransactionSuccessful() {
        this.database.setTransactionSuccessful();
    }

    @Override
    public void execSQL(String sql) throws SQLException {
        Misc.checkNotNullOrEmpty(sql.trim(), "Input SQL");
        this.database.execSQL(sql);
    }

    @Override
    public void execSQL(String sql, Object[] bindArgs) throws SQLException {
        Misc.checkNotNullOrEmpty(sql.trim(), "Input SQL");
        this.database.execSQL(sql, bindArgs);
    }

    @Override
    public int getVersion() {
        return this.database.getVersion();
    }

    @Override
    public int update(String table, ContentValues args, String whereClause, String[] whereArgs) {
        return this.database.update("\""+table+"\"", this.createAndroidContentValues(args), whereClause, whereArgs);
    }

    @Override
    public Cursor rawQuery(String sql, String[] values) {
        return new AndroidSQLiteCursor(this.database.rawQuery(sql, values));
    }

    @Override
    public int delete(String table, String whereClause, String[] whereArgs) {
        return this.database.delete("\""+table+"\"", whereClause, whereArgs);
    }

    @Override
    public long insert(String table, ContentValues args) {
        return this.insertWithOnConflict(table, args, SQLiteDatabase.CONFLICT_NONE);
    }

    @Override
    public long insertWithOnConflict(String table, ContentValues initialValues, int conflictAlgorithm) {
        //android DB will thrown an exception rather than return a -1 row id if there is a failure
        // so we catch constraintException and return -1
        try {
        return this.database.insertWithOnConflict("\""+table+"\"", null,
                createAndroidContentValues(initialValues), conflictAlgorithm);
        } catch (SQLiteConstraintException sqlce){
            return -1;
        }
    }

    private android.content.ContentValues createAndroidContentValues(ContentValues values) {
        android.content.ContentValues newValues = new android.content.ContentValues(values.size());
        for(String key : values.keySet()) {
            Object value = values.get(key);
            if(value instanceof Boolean) {
                newValues.put(key, (Boolean)value);
            } else if(value instanceof Byte) {
                newValues.put(key, (Byte)value);
            } else if(value instanceof byte[]) {
                newValues.put(key, (byte[])value);
            } else if(value instanceof Double) {
                newValues.put(key, (Double)value);
            } else if(value instanceof Float) {
                newValues.put(key, (Float)value);
            } else if(value instanceof Integer) {
                newValues.put(key, (Integer)value);
            } else if(value instanceof Long) {
                newValues.put(key, (Long)value);
            } else if(value instanceof Short) {
                newValues.put(key, (Short)value);
            } else if(value instanceof String) {
                newValues.put(key, (String)value);
            } else if( value == null) {
                newValues.putNull(key);
            } else {
                throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
            }
       }
       return newValues;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if(this.database.isOpen())
            this.database.close();
    }
}
