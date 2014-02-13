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

import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import junit.framework.Assert;

import java.sql.SQLException;
import java.util.Map;

public class IndexTestUtils {

    public static void assertDBObjectNotInIndex(SQLDatabase database, Index index, DocumentRevision o)
            throws SQLException {
        String table = String.format(IndexManager.TABLE_INDEX_NAME_FORMAT, index.getName());
        Cursor cursor = database.rawQuery("SELECT value FROM " + table + " where docid = ?",
                new String[]{ String.valueOf(o.getId()) } );
        Assert.assertFalse(cursor.moveToFirst());
    }

    public static void assertDBObjectInIndex(SQLDatabase database, Index index, String fieldName, DocumentRevision o)
            throws SQLException {
        Map<String, Object> m = o.getBody().asMap();
        Assert.assertTrue(m.containsKey(fieldName));
        Object valueObject = m.get(fieldName);
        assertDBObjectInIndexWithValue(database, index, o, valueObject);
    }

    public static void assertDBObjectInIndexWithValue(
            SQLDatabase database,
            Index index,
            DocumentRevision o,
            Object valueObject) throws SQLException {
        String indexTable = String.format(IndexManager.TABLE_INDEX_NAME_FORMAT, index.getName());
        Cursor cursor = database.rawQuery("SELECT value FROM " + indexTable + " where docid = ?",
                new String[]{ String.valueOf(o.getId()) } );
        Assert.assertTrue(cursor.moveToFirst());
        if (IndexType.STRING == index.getIndexType()) {
            String expectedStringValue = (String) IndexType.STRING.convertToIndexValue(valueObject);
            String stringValue = cursor.getString(0);
            Assert.assertEquals(expectedStringValue, stringValue);
        } else if (IndexType.INTEGER == index.getIndexType()) {
            Long expectedValue = (Long) IndexType.INTEGER.convertToIndexValue(valueObject);
            Long value = cursor.getLong(0);
            Assert.assertEquals(value, expectedValue);
        } else {
            Assert.fail();
        }
    }
}
