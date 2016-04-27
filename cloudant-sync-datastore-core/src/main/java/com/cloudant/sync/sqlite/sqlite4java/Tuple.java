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

package com.cloudant.sync.sqlite.sqlite4java;

import com.cloudant.sync.sqlite.Cursor;

import java.util.ArrayList;
import java.util.List;

/**
 * @api_private
 */
public class Tuple {

    private final List<Integer> desc;
    private final List<Object> values;

    public Tuple(List<Integer> desc) {
        this.desc = desc;
        this.values = new ArrayList<Object>(desc.size());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for(int i = 0 ; i < values.size() ; i ++) {
            sb.append(values.get(i));
            if(i < values.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.append("]").toString();
    }

    public int size() {
        return desc.size();
    }

    public void put(int index) {
        if(desc.get(index) != Cursor.FIELD_TYPE_NULL) {
            throw new IllegalArgumentException("Inserting a null, but expecting " + getTypeName(desc.get(index)));
        }
        this.values.add(index, null);
    }

    public void put(int index, String value) {
        if(desc.get(index) != Cursor.FIELD_TYPE_STRING) {
            throw new IllegalArgumentException("Inserting a string, but expecting " + getTypeName(desc.get(index)));
        }
        this.values.add(index, value);
    }

    // Internally we always store the SQLite number as long
    public void put(int index, long value) {
        if(desc.get(index) != Cursor.FIELD_TYPE_INTEGER) {
            throw new IllegalArgumentException("Inserting an integer, but expecting " + getTypeName(desc.get(index)));
        }
        this.values.add(index, value);
    }

    public void put(int index, float value) {
        if(desc.get(index) != Cursor.FIELD_TYPE_FLOAT) {
            throw new IllegalArgumentException("Inserting a float, but expecting " + getTypeName(desc.get(index)));
        }
        this.values.add(index, value);
    }

    public void put(int index, byte[] value) {
        if(desc.get(index) != Cursor.FIELD_TYPE_BLOB) {
            throw new IllegalArgumentException("Inserting a blob, but expecting " + getTypeName(desc.get(index)));
        }
        this.values.add(index, value);
    }

    public Long getLong(int i) {
        return (Long)this.values.get(i);
    }

    public String getString(int i) {
        return (String)this.values.get(i);
    }

    public byte[] getBlob(int i) {
        return (byte[])this.values.get(i);
    }

    public Float getFloat(int i) {
        return (Float)this.values.get(i);
    }

    // TODO: this is wired, not sure why need this
    public Object getNull(int i) {
        if(desc.get(i) != Cursor.FIELD_TYPE_NULL) {
            throw new IllegalStateException("The file type is not null.");
        }
        return null;
    }

    public Integer getType(int i) {
        return desc.get(i);
    }

    public String getTypeName(int i) {
        switch (i) {
            case Cursor.FIELD_TYPE_NULL:
                return "NULL";
            case Cursor.FIELD_TYPE_BLOB:
                return "Blob";
            case Cursor.FIELD_TYPE_INTEGER:
                return "Integer";
            case Cursor.FIELD_TYPE_FLOAT:
                return "Float";
            case Cursor.FIELD_TYPE_STRING:
                return "String";
        }
        throw new IllegalArgumentException("Unsupported type: " + i);
    }
}
