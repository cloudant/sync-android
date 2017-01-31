/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 */

package com.cloudant.sync.internal.query.callables;

import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.internal.query.QueryImpl;
import com.cloudant.sync.internal.query.ValueExtractor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.Misc;
import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.QueryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Updates a Query Index.
 */
public class UpdateIndexCallable implements SQLCallable<Void> {

    private static final Logger logger = Logger.getLogger(UpdateIndexCallable.class.getName());

    private final Changes changes;
    private final String indexName;
    private final List<FieldSort> fieldNames;

    public UpdateIndexCallable(Changes changes, String indexName, List<FieldSort> fieldNames) {
        this.changes = changes;
        this.indexName = indexName;
        this.fieldNames = fieldNames;
    }

    @Override
    public Void call(SQLDatabase database) throws QueryException {
        for (DocumentRevision rev : changes.getResults()) {
            // Delete existing values
            String tableName = QueryImpl.tableNameForIndex(indexName);
            database.delete(tableName, " _id = ? ", new String[]{rev.getId()});

            // Insert new values if the rev isn't deleted
            if (!rev.isDeleted()) {
                // If we are indexing a document where one field is an array, we
                // have multiple rows to insert into the index.
                List<DBParameter> parameters = parametersToIndexRevision(rev,
                        indexName,
                        fieldNames);
                if (parameters == null) {
                    // non-fatal error found with this rev, but we can carry on indexing
                    continue;
                }
                for (DBParameter parameter : parameters) {
                    long rowId = database.insert(parameter.tableName,
                            parameter.contentValues);
                    if (rowId < 0) {
                        String msg = String.format("Updating index %s failed.", indexName);
                        throw new QueryException(msg);
                    }
                }
            }
        }

        return null;
    }

    /**
     * Returns a List of DBParameters containing table name and ContentValues to index
     * a document in an index.
     *
     * For most revisions, a single entry will be returned. If a field
     * is an array, however, multiple entries are required.
     */
    @SuppressWarnings("unchecked")
    private List<DBParameter> parametersToIndexRevision(DocumentRevision rev,
                                                                     String indexName,
                                                                     List<FieldSort> fieldNames) {
        Misc.checkNotNull(rev, "rev");
        Misc.checkNotNull(indexName, "indexName");
        Misc.checkNotNull(fieldNames, "fieldNames");

        int arrayCount = 0;
        String arrayFieldName = null; // only record the last, as error if more than one
        for (FieldSort fieldName : fieldNames) {
            Object value = ValueExtractor.extractValueForFieldName(fieldName.field, rev.getBody());
            if (value != null && value instanceof List) {
                arrayCount = arrayCount + 1;
                arrayFieldName = fieldName.field;
            }
        }

        if (arrayCount > 1) {
            String msg = String.format("Indexing %s in index %s includes > 1 array field; " +
                            "Only one array field per index allowed.",
                    rev.getId(),
                    indexName);
            logger.log(Level.SEVERE, msg);
            return null;
        }

        List<DBParameter> parameters = new ArrayList<DBParameter>();
        List<Object> arrayFieldValues = null;
        if (arrayCount == 1) {
            arrayFieldValues = (List) ValueExtractor.extractValueForFieldName(arrayFieldName,
                    rev.getBody());
        }

        if (arrayFieldValues != null && arrayFieldValues.size() > 0) {
            for (Object value : arrayFieldValues) {
                // For each value in the list we create a row. We put this value at the start
                // of the INSERT statement along with _id and _rev, followed by the other
                // fields. _id and _rev are special fields in that they don't appear in the
                // body, so they need special-casing to get the values.
                List<FieldSort> initialIncludedFields = Arrays.asList(new FieldSort("_id"),
                        new FieldSort("_rev"),
                        new FieldSort(arrayFieldName));
                List<Object> initialArgs = Arrays.asList(rev.getId(), rev.getRevision(), value);
                DBParameter parameter = populateDBParameter(fieldNames,
                        initialIncludedFields,
                        initialArgs,
                        indexName,
                        rev);
                parameters.add(parameter);
            }
        } else {
            // We know that there is no populated list in the values that we are indexing.
            // We just need to index the fields in the index now. _id and _rev are special
            // fields because they don't appear in the document body, so they need
            // special-casing to get the values.
            List<FieldSort> initialIncludedFields = Arrays.asList(new FieldSort("_id"), new FieldSort("_rev"));
            List<Object> initialArgs = Arrays.<Object>asList(rev.getId(), rev.getRevision());
            DBParameter parameter = populateDBParameter(fieldNames,
                    initialIncludedFields,
                    initialArgs,
                    indexName,
                    rev);
            parameters.add(parameter);
        }

        return parameters;
    }

    private DBParameter populateDBParameter(List<FieldSort> fieldNames,
                                                         List<FieldSort> initialIncludedFields,
                                                         List<Object> initialArgs,
                                                         String indexName,
                                                         DocumentRevision rev) {
        List<FieldSort> includeFieldNames = new ArrayList<FieldSort>();
        includeFieldNames.addAll(initialIncludedFields);
        List<Object> args = new ArrayList<Object>();
        args.addAll(initialArgs);

        for (FieldSort fieldName : fieldNames) {
            // Fields in initialIncludedFields already have values in the other initial* array,
            // so it need not be included again.
            if (initialIncludedFields.contains(fieldName)) {
                continue;
            }

            Object value = ValueExtractor.extractValueForFieldName(fieldName.field, rev.getBody());
            if (value != null && !(value instanceof List && ((List) value).size() == 0)) {
                // Only include a field with a value or a field with a populated list
                includeFieldNames.add(new FieldSort(fieldName.field));
                args.add(value);
            }
        }

        ContentValues contentValues = new ContentValues();
        int argIndex = 0;
        for (FieldSort f : includeFieldNames) {
            String fieldName = f.field;
            fieldName = String.format("\"%s\"", fieldName);
            Object argument = args.get(argIndex);
            if (argument instanceof Boolean) {
                contentValues.put(fieldName, (Boolean) argument);
            } else if (argument instanceof Byte) {
                contentValues.put(fieldName, (Byte) argument);
            } else if (argument instanceof byte[]) {
                contentValues.put(fieldName, (byte[]) argument);
            } else if (argument instanceof Double) {
                contentValues.put(fieldName, (Double) argument);
            } else if (argument instanceof Float) {
                contentValues.put(fieldName, (Float) argument);
            } else if (argument instanceof Integer) {
                contentValues.put(fieldName, (Integer) argument);
            } else if (argument instanceof Long) {
                contentValues.put(fieldName, (Long) argument);
            } else if (argument instanceof Short) {
                contentValues.put(fieldName, (Short) argument);
            } else if (argument instanceof String) {
                contentValues.put(fieldName, (String) argument);
            }
            // NB there is no default case - if the type isn't supported, it doesn't get indexed
            argIndex = argIndex + 1;
        }
        String tableName = QueryImpl.tableNameForIndex(indexName);

        return new DBParameter(tableName, contentValues);
    }

    private static class DBParameter {
        private final String tableName;
        private final ContentValues contentValues;

        public DBParameter(String tableName, ContentValues contentValues) {
            this.tableName = tableName;
            this.contentValues = contentValues;
        }
    }
}
