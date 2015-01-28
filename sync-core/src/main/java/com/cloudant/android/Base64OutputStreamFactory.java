/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.android;

import com.cloudant.sync.util.Misc;

import java.io.OutputStream;
import java.lang.reflect.Constructor;

/**
 * Created by tomblench on 07/07/2014.
 */
public class Base64OutputStreamFactory {
    public static OutputStream get(OutputStream os) {
        try {
            if (Misc.isRunningOnAndroid()) {
                Class c = Class.forName("android.util.Base64OutputStream");
                Constructor ctor = c.getDeclaredConstructor(OutputStream.class, int.class);
                return (OutputStream)ctor.newInstance(os, 0);
            } else {
                Class c = Class.forName("org.apache.commons.codec.binary.Base64OutputStream");
                Constructor ctor = c.getDeclaredConstructor(OutputStream.class, boolean.class, int.class, byte[].class);
                return (OutputStream)ctor.newInstance(os, true, 0, null);
            }
        } catch (Exception e) {
            // TODO log
            return null;
        }
    }
}
