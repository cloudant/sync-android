/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.sqlite.sqlite4java;

import com.cloudant.sync.internal.sqlite.Cursor;

import java.util.List;
import java.util.Locale;

/**
 * @api_private
 */
public class SQLiteCursor implements Cursor {

    private int position = -1;
    private final int count;
    private final List<String> names;
    private final List<Tuple> data;

    public SQLiteCursor(List<String> names, List<Tuple> data) {

        if(data.size() > 0) {
            assert names.size() == data.get(0).size();
        }

        this.names = names;
        this.data = data;

        this.count = data.size();
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public int getColumnCount() {
        return this.names.size();
    }

    @Override
    public int columnType(int i) {
        return data.get(position).getType(i);
    }

    @Override
    public String columnName(int i) {
        return names.get(i);
    }

    @Override
    public boolean moveToFirst() {
        position = 0;
        return count > 0;
    }

    @Override
    public float getFloat(int index) {
        return getData().getFloat(index);
    }

    @Override
    public String getString(int index) {
        return getData().getString(index);
    }

    @Override
    public int getInt(int index) {
        return getData().getLong(index).intValue();
    }

    @Override
    public long getLong(int index) {
        return getData().getLong(index);
    }

    @Override
    public byte[] getBlob(int index) {
        return getData().getBlob(index);
    }

    @Override
    public boolean isAfterLast() {
        return this.position >= count;
    }

    @Override
    public boolean moveToNext() {
        this.position ++;
        return !isAfterLast();
    }

    @Override
    public void close() {
    }

    Tuple getData() {
        return this.data.get(this.position);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SQLiteCursor: ");
        sb.append("count ").append(this.getCount());
        sb.append(", columnCount ").append(this.getColumnCount());
        sb.append(", names ").append(this.names);
        return sb.toString();
    }

    @Override
    public int getColumnIndex(String columnName) {
        int index = -1;
        index = names.indexOf(columnName);
        if (index != -1) {
            return index;
        }
        index = names.indexOf(columnName.toUpperCase(Locale.ENGLISH));
        if (index != -1) {
            return index;
        }
        index = names.indexOf(columnName.toLowerCase(Locale.ENGLISH));
        if (index != -1) {
            return index;
        }
        return index;

    }

    @Override
    public int getColumnIndexOrThrow(String columnName) throws IllegalArgumentException {
        int i = getColumnIndex(columnName);
        if(i < 0) {
            throw new IllegalArgumentException("Can not find column: " + columnName);
        } else {
            return i;
        }
    }

}
