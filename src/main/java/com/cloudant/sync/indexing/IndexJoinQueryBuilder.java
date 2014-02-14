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

import java.util.List;
import java.util.Map;

class IndexJoinQueryBuilder {

    public static final String MIN = "min";
    public static final String MAX = "max";

    public static final String SELECT_CLAUSE_FORMAT = "SELECT DISTINCT %s.docid";
    public static final String FROM_CLAUSE_FORMAT = " FROM %s";
    public static final String JOIN_CLAUSE_FORMAT = " JOIN %s ON %s.docid = %s.docid";
    public static final String ORDERBY_CLAUSE_FORMAT = " ORDER BY %s.value %s";

    public static final String WHERE = " WHERE";
    public static final String AND = " AND";

    public static final String EQUALS_CLAUSE = " %s.value = %s";
    public static final String IN_CLAUSE = " %s.value IN (%s)";
    public static final String MIN_MAX_CLAUSE = " %s.value < %s AND %s.value > %s";
    public static final String MIN_CLAUSE = " %s.value > %s";
    public static final String MAX_CLAUSE = " %s.value < %s";

    String firstTable;
    String from;
    StringBuilder join;
    StringBuilder where;
    StringBuilder orderby;

    public IndexJoinQueryBuilder() {
        from = null;
        join = new StringBuilder();
        where = new StringBuilder();
        orderby = new StringBuilder();
    }

    public String toSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format(SELECT_CLAUSE_FORMAT, firstTable)).append(from).append(join).append(where).append(orderby);
        return sb.toString();
    }

    public void addQueryCriterion(String table, Object criterion, IndexType type) {
        if (firstTable == null) {
            firstTable = table;
        }
        if (from == null) {
            from = String.format(FROM_CLAUSE_FORMAT, table);
            where.append(WHERE).append(buildWherePartClause(table, criterion, type));
        } else {
            join.append(String.format(JOIN_CLAUSE_FORMAT, table, firstTable, table));
            where.append(AND).append(buildWherePartClause(table, criterion, type));
        }
    }

    public void addJoinForSort(String table) {
        join.append(String.format(JOIN_CLAUSE_FORMAT, table, firstTable, table));
    }

    public void addSortByOption(String value, SortDirection direction) {
        String directionString = direction == SortDirection.Ascending ? "asc" : "desc";
        orderby.append(String.format(ORDERBY_CLAUSE_FORMAT, value, directionString));
    }

    public void addOffsetOption(int offset) {
    }

    public void addLimitOption(int limit) {
    }

    private String buildWherePartClause(String index, Object criterion, IndexType type) {
        String where;
        if (criterion instanceof List) {
            where = constructWherePartWithList(index, (List) criterion, type);
        } else if (criterion instanceof Map) {
            where = constructWhereWithMap(index, (Map) criterion, type);
        } else if (type.valueSupported(criterion)) {
            where = constructSimpleWherePart(index, criterion, type);
        } else {
            throw new IllegalArgumentException("Unsupported criterion object: "
                    + criterion.getClass() + " for index type: " + type);
        }
        return where;
    }

    private String constructWhereWithMap(String index, Map criterion, IndexType type) {
        String where;
        if (criterion.containsKey(MIN) && criterion.containsKey(MAX)) {
            where = String.format(MIN_MAX_CLAUSE, index, type.escape(criterion.get(MAX)), index, type.escape(criterion.get(MIN)));
        } else if (criterion.containsKey(MIN)) {
            where = String.format(MIN_CLAUSE, index, type.escape(criterion.get(MIN)));
        } else if (criterion.containsKey(MAX)) {
            where = String.format(MAX_CLAUSE, index, type.escape(criterion.get(MAX)));
        } else {
            throw new IllegalArgumentException("Range query must have at least one of min or max value");
        }
        return where;
    }

    private String convertAndEscape(IndexType type, Object object) {
        Object c = type.convertToIndexValue(object);
        return type.escape(c);
    }

    private String constructWherePartWithList(String idx, List criterion, IndexType type) {
        StringBuilder sb = new StringBuilder();
        if (criterion.size() > 0) {
            for (Object obj : criterion) {
                if (type.valueSupported(obj)) {
                    sb.append(convertAndEscape(type, obj)).append(",");
                }
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        return String.format(IN_CLAUSE, idx, sb.toString());
    }

    private String constructSimpleWherePart(String idx, Object criterion, IndexType type) {
        return String.format(EQUALS_CLAUSE, idx, convertAndEscape(type, criterion));
    }
}
