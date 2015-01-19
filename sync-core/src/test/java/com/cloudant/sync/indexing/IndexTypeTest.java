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

import org.junit.Assert;
import org.junit.Test;

public class IndexTypeTest {

    @Test
    public void integerIndex_valueSupported_allNumberAreSupported() {
        Object[] valueObjects = new Object[] {new Byte((byte)127), new Short((short)100), new Integer(100), new Long(100l), new Float(100.0), new Double(100.0), "100"} ;
        Boolean[] supported = new Boolean[] {Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE};

        for(int i = 0 ; i < valueObjects.length ; i ++) {
            Boolean res = IndexType.INTEGER.valueSupported(valueObjects[i]);
            Assert.assertTrue(supported[i].equals(res));
        }
    }

    @Test
    public void stringIndex_valueSupported_onlyStringIsSupported() {
        Object[] valueObjects = new Object[] {new Integer(100), new Long(100l), "100", new Float(100)} ;
        Boolean[] supported = new Boolean[]{Boolean.FALSE, Boolean.FALSE, Boolean.TRUE, Boolean.FALSE};

        for(int i = 0 ; i < valueObjects.length ; i ++) {
            Boolean res = IndexType.STRING.valueSupported(valueObjects[i]);
            Assert.assertTrue(supported[i].equals(res));
        }
    }

    private void assertValueObjectConvertedCorrectly(Object[] valueObjects, Long[] expected) {
        for(int i = 0 ; i < valueObjects.length ; i ++) {
            Long res = (Long) IndexType.INTEGER.convertToIndexValue(valueObjects[i]);
            Assert.assertTrue(expected[i].equals(res));
        }
    }

    @Test
    public void integerIndex_convertToIndexValue_byte() {
        Object[] valueObjects = new Object[] {new Byte((byte)127), new Byte((byte)-127)};
        Long[] expected = new Long[]{127l, -127l};

        assertValueObjectConvertedCorrectly(valueObjects, expected);
    }

    @Test
    public void integerIndex_convertToIndexValue_short() {
        Object[] valueObjects = new Object[] {new Short((short)100), new Short((short)-100)};
        Long[] expected = new Long[]{100l, -100l};

        assertValueObjectConvertedCorrectly(valueObjects, expected);
    }

    @Test
    public void integerIndex_convertToIndexValue_integer() {
        Object[] valueObjects = new Object[] {new Integer(100), new Integer(-100)};
        Long[] expected = new Long[]{100l, -100l};

        assertValueObjectConvertedCorrectly(valueObjects, expected);
    }

    @Test
    public void integerIndex_convertToIndexValue_long() {
        Object[] valueObjects = new Object[] {new Long(100l), new Long(-100l)};
        Long[] expected = new Long[]{100l, -100l};

        assertValueObjectConvertedCorrectly(valueObjects, expected);
    }


    @Test
    public void integerIndex_convertToIndexValue_float() {
        Object[] valueObjects = new Object[] {new Double(100.9), new Double(99.1), new Double(-100.1)};
        Long[] expected = new Long[]{100l, 99l, -100l};

        assertValueObjectConvertedCorrectly(valueObjects, expected);
    }

    @Test
    public void integerIndex_convertToIndexValue_double() {
        Object[] valueObjects = new Object[] {new Double(100.9), new Double(100.1), new Double(-100.1)};
        Long[] expected = new Long[]{100l, 100l, -100l};

        assertValueObjectConvertedCorrectly(valueObjects, expected);
    }

    @Test
    public void stringIndex_convertToIndexValue_onlyStringIsSupported() {
        Object[] valueObjects = new Object[] {"102"};
        String[] expected = new String[]{"102"};

        for(int i = 0 ; i < valueObjects.length ; i ++) {
            String res = (String) IndexType.STRING.convertToIndexValue(valueObjects[i]);
            Assert.assertTrue(expected[i].equals(res));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void convertToIndexValue_StringValueForIntegerIndex_exception() {
        IndexType.INTEGER.convertToIndexValue(new String("100"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convertToStringValue_NonStringValueForStringIndex_exception() {
        IndexType.STRING.convertToIndexValue(new Integer(100));
    }

    @Test
    public void stringIndex_escapeSingleQuote() {
        String expected = "''''";
        String result = IndexType.STRING.escape("'");
        Assert.assertEquals(expected, result);
    }
}
