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

package com.cloudant.sync.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class MiscTest {

    @Test
    public void uuid_noDash() {
        String uuid = Misc.createUUID();
        Assert.assertTrue(uuid.indexOf("_") < 0);
        Assert.assertEquals(32, uuid.length());
    }

    @Test
    public void isRunningOnAndroid_returnFalse() {

        if(Misc.isRunningOnAndroid())
            return; //this test is invalid for android

        Boolean runningOnAndroid = Misc.isRunningOnAndroid();
        Assert.assertFalse(runningOnAndroid);
    }

    @Test
    public void isRunningOnAndroid_returnTrue() {
        String p = System.getProperty("java.runtime.name");
        try {
            System.setProperty("java.runtime.name", "Android Runtime");
            Assert.assertTrue(Misc.isRunningOnAndroid());
        } finally {
            System.setProperty("java.runtime.name", p);
        }
    }

    @Test
    public void joinSkipsNulls() throws Exception {
        String joined =
                Misc.join("##", Arrays.asList(new String[]{"alpha", null, "beta", null, "gamma"}));
        Assert.assertEquals("The joined string should not have null elements",
                "alpha##beta##gamma", joined);
    }

    @Test
    public void joinDoesNotAppendTrailingSeparator() throws Exception {
        String joined = Misc.join("##", Arrays.asList(new String[]{"alpha"}));
        Assert.assertFalse("The joined string should not end with the separator", joined.endsWith
                ("##"));

        joined = Misc.join("##", Arrays.asList(new String[]{"alpha", "beta"}));
        Assert.assertFalse("The joined string should not end with the separator", joined.endsWith
                ("##"));
    }

    @Test
    public void basicJoin() throws Exception {
        String joined = Misc.join(", ", Arrays.asList(new String[]{"alpha", "beta", "gamma"}));
        Assert.assertEquals("Should get the correct joined string",
                "alpha, beta, gamma", joined);
    }

    @Test
    public void defaultNullStringCheck() throws Exception {
        try {
            Misc.checkNotNullOrEmpty(null, null);
        } catch(IllegalArgumentException e) {
            Assert.assertEquals("Parameter must not be null.", e.getMessage());
        }
    }

    @Test
    public void defaultEmptyStringCheck() throws Exception {
        try {
            Misc.checkNotNullOrEmpty("", null);
        } catch(IllegalArgumentException e) {
            Assert.assertEquals("Parameter must not be empty.", e.getMessage());
        }
    }

    @Test
    public void defaultNullObjectCheck() throws Exception {
        try {
            Misc.checkNotNull(null, null);
        } catch(NullPointerException e) {
            Assert.assertEquals("Parameter must not be null.", e.getMessage());
        }
    }

    @Test
    public void nullStringCheck() throws Exception {
        try {
            Misc.checkNotNullOrEmpty(null, "Test argument");
        } catch(IllegalArgumentException e) {
            Assert.assertEquals("Test argument must not be null.", e.getMessage());
        }
    }

    @Test
    public void emptyStringCheck() throws Exception {
        try {
            Misc.checkNotNullOrEmpty("", "Test argument");
        } catch(IllegalArgumentException e) {
            Assert.assertEquals("Test argument must not be empty.", e.getMessage());
        }
    }

    @Test
    public void nullObjectCheck() throws Exception {
        try {
            Misc.checkNotNull(null, "Test argument");
        } catch(NullPointerException e) {
            Assert.assertEquals("Test argument must not be null.", e.getMessage());
        }
    }

    @Test
    public void stringNull() throws Exception {
        Assert.assertTrue(Misc.isStringNullOrEmpty(null));
    }

    @Test
    public void stringEmpty() throws Exception {
        Assert.assertTrue(Misc.isStringNullOrEmpty(""));
    }

    @Test
    public void stringNeitherNullNorEmpty() throws Exception {
        Assert.assertFalse(Misc.isStringNullOrEmpty("hello"));
    }

    @Test
    public void checkTrueArgument() throws Exception {
        Misc.checkArgument(true, "Whatever");
    }

    @Test
    public void checkFalseArgument() throws Exception {
        try {
            Misc.checkArgument(false, "Whatever");
        } catch(IllegalArgumentException e) {
            Assert.assertEquals("Whatever", e.getMessage());
        }
    }

    @Test
    public void checkTrueState() throws Exception {
        Misc.checkState(true, "Whatever");
    }

    @Test
    public void checkFalseState() throws Exception {
        try {
            Misc.checkState(false, "Whatever");
        } catch(IllegalStateException e) {
            Assert.assertEquals("Whatever", e.getMessage());
        }
    }
}
