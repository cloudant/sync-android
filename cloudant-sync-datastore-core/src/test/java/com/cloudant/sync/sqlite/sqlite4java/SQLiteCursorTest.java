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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SQLiteCursorTest {

    static List<Integer> types;
    static List<String> names;
    static List<Tuple> data;
    SQLiteCursor cursor;

    @BeforeClass
    public static void onceSetup () {
        types = createTupleDesc();
        names = createTupleNames();
        data = new ArrayList<Tuple>();
        data.add(createTuple1(types));
        data.add(createTuple2(types));
    }

    @Before
    public void setup() {
        cursor = new SQLiteCursor(names, data);
    }

    @Test
    public void getCount() {
        Assert.assertTrue(cursor.getCount() == 2);
    }

    @Test
    public void getColumnType() {
        cursor.moveToFirst();
        Assert.assertTrue(cursor.columnType(0) == Cursor.FIELD_TYPE_BLOB);
        Assert.assertTrue(cursor.columnType(1) == Cursor.FIELD_TYPE_STRING);
        Assert.assertTrue(cursor.columnType(4) == Cursor.FIELD_TYPE_NULL);
    }

    @Test
    public void getColumnName() {
        Assert.assertTrue(cursor.columnName(0).equals("column 0"));
        Assert.assertTrue(cursor.columnName(4).equals("column 4"));
    }

    private static List<Integer> createTupleDesc() {
        List<Integer> desc = new ArrayList<Integer>();
        desc.add(0, Cursor.FIELD_TYPE_BLOB);
        desc.add(1, Cursor.FIELD_TYPE_STRING);
        desc.add(2, Cursor.FIELD_TYPE_FLOAT);
        desc.add(3, Cursor.FIELD_TYPE_INTEGER);
        desc.add(4, Cursor.FIELD_TYPE_NULL);
        return desc;
    }

    private static List<String> createTupleNames() {
        List<String> desc = new ArrayList<String>();
        desc.add(0, "column 0");
        desc.add(1, "column 1");
        desc.add(2, "column 2");
        desc.add(3, "column 3");
        desc.add(4, "column 4");
        return desc;
    }

    private static Tuple createTuple1(List<Integer> types) {
        Tuple t = new Tuple(types);
        t.put(0, new byte[]{'a', 'b'});
        t.put(1, "haha");
        t.put(2, 102.0F);
        t.put(3, 103);
        t.put(4);
        return t;
    }

    private static Tuple createTuple2(List<Integer> types) {
        Tuple t = new Tuple(types);
        t.put(0, new byte[]{'b', 'c', 'd'});
        t.put(1, "hehe");
        t.put(2, 103.0F);
        t.put(3, 105);
        t.put(4);
        return t;
    }

    @Test
    public void travers() {
        Assert.assertTrue(cursor.moveToNext());
        Assert.assertTrue(Arrays.equals(new byte[]{'a', 'b'}, cursor.getBlob(0)));
        Assert.assertEquals("haha", cursor.getString(1));
        Assert.assertEquals(102.0F, cursor.getFloat(2), 0.000001F);
        Assert.assertEquals(103, cursor.getInt(3));

        Assert.assertTrue(cursor.moveToNext());
        Assert.assertTrue(Arrays.equals(new byte[]{'b', 'c', 'd'}, cursor.getBlob(0)));
        Assert.assertEquals("hehe", cursor.getString(1));
        Assert.assertEquals(103.0F, cursor.getFloat(2), 0.000001F);
        Assert.assertEquals(105, cursor.getInt(3));

        Assert.assertFalse(cursor.moveToNext());
    }

}
