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

import com.cloudant.sync.internal.util.Misc;

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
}
