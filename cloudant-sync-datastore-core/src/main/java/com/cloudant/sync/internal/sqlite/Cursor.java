/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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

package com.cloudant.sync.internal.sqlite;

public interface Cursor {

    /** Value returned by {@link #columnType(int)} if the specified column is null */
    public static final int FIELD_TYPE_NULL = 0;

    /** Value returned by {@link #columnType(int)} if the specified  column type is integer */
    public static final int FIELD_TYPE_INTEGER = 1;

    /** Value returned by {@link #columnType(int)} if the specified column type is float */
    public static final int FIELD_TYPE_FLOAT = 2;

    /** Value returned by {@link #columnType(int)} if the specified column type is string */
    public static final int FIELD_TYPE_STRING = 3;

    /** Value returned by {@link #columnType(int)} if the specified column type is blob */
    public static final int FIELD_TYPE_BLOB = 4;

    public int getCount();

    /**
     * Return total number of columns
     */
    int getColumnCount();

    /**
     * Returns data type of the given column's value.
     */
    public int columnType(int index);

    /**
     * Returns the column name at the given zero-based column index
     */
    public String columnName(int index);

    /**
     * Move the cursor to the first row.
     * This method will return false if the cursor is empty.
     * @return whether the move succeeded.
     */
    public boolean moveToFirst() ;

    /**
     * Returns the value of the requested column as a String.
     * Throws an exception when the column value is null or the column type is not a string type is implementation-defined.
     */
    public String getString(int index);

    /**
     * Returns the value of the requested column as a long.
     */
    public int getInt(int index);

    /**
     * Returns the value of the requested column as a long.
     */
    public long getLong(int index);

    /**
     * Returns the value of the requested column as a float.
     */
    public float getFloat(int index);

    /**
     * Returns the value of the requested column as a byte array.
     */
    public byte[] getBlob(int index);

    /**
     * Returns whether the cursor is pointing to the position after the last row.
     */
    public boolean isAfterLast() ;

    /**
     * Move the cursor to the next row.
     * This method will return false if the cursor is already past the last entry in the result set.
     */
    public boolean moveToNext();

    /**
     * Close the cursor and release all resource associated
     */
    public void close();

    /**
     * Returns the zero-based index for the given column name, or -1 if the column doesn't exist.
     * If you expect the column to exist use {@link #getColumnIndexOrThrow(String)} instead, which
     * will make the error more clear.
     *
     * @param columnName the name of the target column.
     * @return the zero-based column index for the given column name, or -1 if
     * the column name does not exist.
     * @see #getColumnIndexOrThrow(String)
     */
    public int getColumnIndex(String columnName);

    /**
     * Returns the zero-based index for the given column name, or throws
     * {@link IllegalArgumentException} if the column doesn't exist. If you're not sure if
     * a column will exist or not use {@link #getColumnIndex(String)} and check for -1, which
     * is more efficient than catching the exceptions.
     *
     * @param columnName the name of the target column.
     * @return the zero-based column index for the given column name
     * @see #getColumnIndex(String)
     * @throws IllegalArgumentException if the column does not exist
     */
    public int getColumnIndexOrThrow(String columnName) throws IllegalArgumentException;
}
