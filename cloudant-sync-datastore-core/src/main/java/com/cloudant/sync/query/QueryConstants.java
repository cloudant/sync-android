//  Copyright (c) 2015 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

public final class QueryConstants {

    public static final String AND = "$and";

    public static final String OR = "$or";

    public static final String NOT = "$not";

    public static final String EXISTS = "$exists";

    public static final String TEXT = "$text";

    public static final String EQ = "$eq";

    public static final String NE = "$ne";

    public static final String LT = "$lt";

    public static final String LTE = "$lte";

    public static final String GT = "$gt";

    public static final String GTE = "$gte";

    public static final String IN = "$in";

    public static final String NIN = "$nin";

    public static final String SEARCH = "$search";

    public static final String MOD = "$mod";

    public static final String SIZE = "$size";

    private static final String INDEX_TABLE_PREFIX = "_t_cloudant_sync_query_index_";

    public static final String INDEX_METADATA_TABLE_NAME = "_t_cloudant_sync_query_metadata";

    private QueryConstants() {
        throw new AssertionError();
    }

    public static String[] getSchemaVersion1() {
        return new String[] {
                "CREATE TABLE " + INDEX_METADATA_TABLE_NAME + " ( " +
                "        index_name TEXT NOT NULL, " +
                "        index_type TEXT NOT NULL, " +
                "        field_name TEXT NOT NULL, " +
                "        last_sequence INTEGER NOT NULL);"
        };
    }

    public static String[] getSchemaVersion2() {
        return new String[] {
                "ALTER TABLE " + INDEX_METADATA_TABLE_NAME +
                "        ADD COLUMN index_settings TEXT NULL;"
        };
    }

    public static String tableNameForIndex(String indexName) {
        return INDEX_TABLE_PREFIX.concat(indexName);
    }

}
