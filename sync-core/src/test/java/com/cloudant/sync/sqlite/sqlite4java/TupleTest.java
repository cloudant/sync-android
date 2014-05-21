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
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TupleTest {

    @Test
    public void tuple_test() {

        List<Integer> desc = createTupleDesc();

        Tuple t = new Tuple(desc);
        t.put(0, new byte[]{ 'a', 'b' });
        t.put(1, "haha");
        t.put(2, 102.0F);
        t.put(3, 103);
        t.put(4);

        Assert.assertTrue(t.getBlob(0).length == 2);
        Assert.assertTrue(t.getBlob(0)[0] == 'a');
        Assert.assertTrue(t.getBlob(0)[1] == 'b');

        Assert.assertTrue(t.getString(1).equals("haha"));
        Assert.assertTrue(t.getFloat(2).equals(102.0F));
        Assert.assertEquals(Long.valueOf(103l), t.getLong(3));
        Assert.assertNull(t.getNull(4));
    }

    private List<Integer> createTupleDesc() {
        List<Integer> desc = new ArrayList<Integer>();
        desc.add(0, Cursor.FIELD_TYPE_BLOB);
        desc.add(1, Cursor.FIELD_TYPE_STRING);
        desc.add(2, Cursor.FIELD_TYPE_FLOAT);
        desc.add(3, Cursor.FIELD_TYPE_INTEGER);
        desc.add(4, Cursor.FIELD_TYPE_NULL);
        return desc;
    }

    @Test(expected = IllegalArgumentException.class)
    public void tuple_wrongValueType() {
        List<Integer> desc = createTupleDesc();
        Tuple t = new Tuple(desc);
        t.put(0, "haha");
    }
}
