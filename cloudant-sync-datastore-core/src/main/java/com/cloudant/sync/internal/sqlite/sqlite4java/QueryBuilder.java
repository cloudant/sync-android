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

package com.cloudant.sync.internal.sqlite.sqlite4java;

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.internal.util.Misc;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * @api_private
 */
public class QueryBuilder {

    private static final Pattern sLimitPattern =
            Pattern.compile("\\s*\\d+\\s*(,\\s*\\d+\\s*)?");

    public static String buildUpdateQuery(String table, ContentValues values, String whereClause, String[] whereArgs) {

        StringBuilder query = new StringBuilder(120);
        query.append("UPDATE ")
                .append("\"")
                .append(table)
                .append("\"")
                .append(" SET ");

        int i = 0;
        for (String colName : values.keySet()) {
            query.append((i > 0) ? "," : "");
            query.append(colName);
            query.append("=?");
            i++;
        }

        if (!Misc.isStringNullOrEmpty(whereClause)) {
            query.append(" WHERE ");
            query.append(whereClause);
        }

        return query.toString();
    }

    public static Object[] buildBindArguments(ContentValues values, String[] whereArgs) {
        ArrayList<Object> bindArgs = new ArrayList<Object>();

        for (String colName : values.keySet()) {
            bindArgs.add(values.get(colName));
        }

        if (whereArgs != null) {
            for (String whereArg : whereArgs) {
                bindArgs.add(whereArg);
            }
        }

        return bindArgs.toArray();
    }

    public static String buildSelectCountQuery(String table, String where) {
        StringBuilder query = new StringBuilder(120);
        query.append("SELECT count(*) FROM \"");
        query.append(table);
        query.append("\"");
        if(where != null) {
            appendClause(query, " WHERE ", where);
        }
        return query.toString();
    }

    public static String buildSelectQuery(String table, String[] columns, String where,
            String groupBy, String having, String orderBy, String limit) {

        if (Misc.isStringNullOrEmpty(groupBy) && !Misc.isStringNullOrEmpty(having)) {
            throw new IllegalArgumentException(
                    "HAVING clauses are only permitted when using a groupBy clause");
        }
        if (!Misc.isStringNullOrEmpty(limit) && !sLimitPattern.matcher(limit).matches()) {
            throw new IllegalArgumentException("invalid LIMIT clauses:" + limit);
        }

        StringBuilder query = new StringBuilder(120);

        query.append("SELECT ");
        if (columns != null && columns.length != 0) {
            appendColumns(query, columns);
        } else {
            query.append("* ");
        }
        query.append("FROM ")
                .append("\"")
                .append(table)
                .append("\"");

        if(where != null) {
            appendClause(query, " WHERE ", where);
        }

        if(groupBy != null) {
            appendClause(query, " GROUP BY ", groupBy);
        }

        if(having != null) {
            appendClause(query, " HAVING ", having);
        }

        if(orderBy != null) {
            appendClause(query, " ORDER BY ", orderBy);
        }

        if(limit != null) {
            appendClause(query, " LIMIT ", limit);
        }

        return query.toString();
    }

    /**
     * Add the names that are non-null in columns to s, separating
     * them with commas.
     */
    static void appendColumns(StringBuilder s, String[] columns) {
        int n = columns.length;
        for (int i = 0; i < n; i++) {
            String column = columns[i];
            if (column != null) {
                if (i > 0) {
                    s.append(", \"");
                }
                s.append(column);
                s.append("\"");
            }
        }
        s.append(' ');
    }

    static void appendClause(StringBuilder s, String name, String clause) {
        if (!Misc.isStringNullOrEmpty(clause)) {
            s.append(name);
            s.append(clause);
        }
    }

}
