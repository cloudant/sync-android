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

package com.cloudant.sync.indexing;

import com.cloudant.sync.sqlite.ContentValues;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * <p>
 * The datatype used for an index.
 * </p>
 * <p>
 * {@link IndexType#INTEGER}: supports any {@code java.lang.Number}
 * subclass. Before indexing, all {@code java.lang.Number} types are converted
 * to {@code java.lang.Long} using: {@code java.lang.Number.longValue()}.
 * </p>
 * <p>
 * {@link IndexType#STRING}: supports only {@code java.lang.String} are supported.
 * No conversion is performed.
 * </p>
 */
public enum IndexType {
    /**
     * <p>
     * Supports any {@code java.lang.Number}
     * subclass. Before indexing, all {@code java.lang.Number} types are converted
     * to {@code java.lang.Long} using: {@code java.lang.Number.longValue()}.
     * </p>
     */
    INTEGER {
        @Override
        boolean valueSupported(Object object) {
            return object instanceof Number;
        }

        @Override
        Long convertToIndexValue(Object object) {
            Preconditions.checkArgument(valueSupported(object),
                    "Object is not supported for this index type");
            return ((Number) object).longValue();
        }

        @Override
        String escape(Object obj) {
            Preconditions.checkArgument(valueSupported(obj), "Object is not supported for this index type");
            return obj.toString();
        }

        @Override
        String createSQLTemplate(String tablePrefix, String indexName) {
            IndexManager.validateIndexName(indexName);
            String tableName = tablePrefix + indexName;
            final String SQL_INTEGER_INDEX = "CREATE TABLE %s ( " +
                    "docid TEXT NOT NULL, " +
                    "value INTEGER NOT NULL, " +
                    "UNIQUE(docid, value) ON CONFLICT IGNORE ); " +
                    "CREATE INDEX %s_value_docid ON %s(value, docid);";
            return String.format(
                    SQL_INTEGER_INDEX,
                    tableName,
                    tableName,
                    tableName
            );
        }

        @Override
        void putIntoContentValues(ContentValues cv, String key, java.lang.Object o) {
            cv.put(key, this.convertToIndexValue(o));
        }
    },

    /**
     * <p>
     * Supports only {@code java.lang.String} values.</p>
     *
     * <p>No conversion is performed, so inserting non-String values into the
     * index will fail.</p>
     */
    STRING {
        @Override
        boolean valueSupported(Object value) {
            return (value instanceof String && !Strings.isNullOrEmpty((String)value));
        }

        @Override
        String convertToIndexValue(Object object) {
            Preconditions.checkArgument(valueSupported(object), "Object is not supported for this index type");
            return (String) object;
        }

        @Override
        String escape(Object obj) {
            Preconditions.checkArgument(valueSupported(obj), "Object is not supported for this index type");
            StringBuilder sb = new StringBuilder().append("'").append(escapeSingleQuote((String) obj)).append("'");
            return sb.toString();
        }

        @Override
        String createSQLTemplate(String tablePrefix, String indexName) {
            IndexManager.validateIndexName(indexName);
            String tableName = tablePrefix + indexName;
            final String SQL_STRING_INDEX = "CREATE TABLE %s ( " +
                    "docid TEXT NOT NULL, " +
                    "value TEXT NOT NULL, " +
                    "UNIQUE(docid, value) ON CONFLICT IGNORE ); " +
                    "CREATE INDEX %s_value_docid ON %s(value, docid);";
            return String.format(
                    SQL_STRING_INDEX,
                    tableName,
                    tableName,
                    tableName
            );
        }

        @Override
        void putIntoContentValues(ContentValues cv, String key, java.lang.Object o) {
            cv.put(key, this.convertToIndexValue(o));
        }
    };

    private static String escapeSingleQuote(String input) {
        return input.replace("'", "''");
    }

    /**
     * Returns the template for index table generation within the SQL database.
     *
     * @return SQL string for creating the index in the database.
     */
    abstract String createSQLTemplate(String tablePrefix, String indexName);

    /**
     * <p>
     * Converts the given {@code Object} to a value suitable for inserting
     * into the index.
     * </p>
     * <p>Each {@code IndexType} converts values to index into a uniform
     * data type before the value is inserted into the index. Different
     * (@code IndexType} classes convert into different data types before
     * inserting into the index.</p>
     *
     * @param object value to convert
     * @return converted value
     */
    abstract Object convertToIndexValue(Object object);

    /**
     * Returns {@code true} if the given {@code Object} is a data type supported
     * by the index.
     */
    abstract boolean valueSupported(Object object);

    /**
     * Escapes an object for use in a SQL {@code WHERE} clause.
     *
     * <p>The input object should already have been converted to the right data
     * type.</p>
     *
     * @return Escaped string safe  for use in a SQL query.
     */
    abstract String escape(Object object);

    /**
     * Adds the given value to the ContentValues map in a type-safe manner.
     *
     * <p>A {@code ContentValues} object has typed insert methods, so we need
     * to use the appropriate one. This methods checks the class of {@code o}
     * and selects the right method.</p>
     *
     * @param cv ContentValues object to insert into.
     * @param o Object to insert.
     */
    abstract void putIntoContentValues(ContentValues cv, String key, java.lang.Object o);
}
