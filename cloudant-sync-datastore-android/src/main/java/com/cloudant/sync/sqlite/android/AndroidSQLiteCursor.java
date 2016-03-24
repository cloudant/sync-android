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

package com.cloudant.sync.sqlite.android;

import com.cloudant.sync.sqlite.Cursor;

public class AndroidSQLiteCursor implements Cursor {

    private android.database.Cursor internalCursor;

    public AndroidSQLiteCursor(android.database.Cursor cursor) {
        this.internalCursor = cursor;
    }

    @Override
    public int getCount() {
        return this.internalCursor.getCount();
    }

    @Override
    public int columnType(int index) {
        return this.internalCursor.getType(index);
    }

    @Override
    public String columnName(int index) {
        return this.internalCursor.getColumnName(index);
    }

    @Override
    public boolean moveToFirst() {
        return this.internalCursor.moveToFirst();
    }

    @Override
    public String getString(int index) {
        return this.internalCursor.getString(index);
    }

    @Override
    public int getInt(int index) {
        return this.internalCursor.getInt(index);
    }

    @Override
    public long getLong(int index) {
        return this.internalCursor.getLong(index);
    }

    @Override
    public float getFloat(int index) {
        return this.internalCursor.getFloat(index);
    }

    @Override
    public byte[] getBlob(int index) {
        return this.internalCursor.getBlob(index);
    }

    @Override
    public boolean isAfterLast() {
        return this.internalCursor.isAfterLast();
    }

    @Override
    public boolean moveToNext() {
        return this.internalCursor.moveToNext();
    }

    @Override
    public void close() {
        this.internalCursor.close();
    }

    @Override
    public int getColumnCount() {
        return this.internalCursor.getColumnCount();
    }

    @Override
    public int getColumnIndex(String columnName){
        return this.internalCursor.getColumnIndex(columnName);
    }

    @Override
    public int getColumnIndexOrThrow(String columnName) throws IllegalArgumentException {
        return this.internalCursor.getColumnIndexOrThrow(columnName);
    }

}
