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

import junit.framework.Assert;
import org.junit.Test;

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
}
