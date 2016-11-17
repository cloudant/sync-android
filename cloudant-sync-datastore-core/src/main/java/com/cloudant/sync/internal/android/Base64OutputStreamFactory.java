/*
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.android;

import com.cloudant.sync.internal.util.Misc;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by tomblench on 07/07/2014.
 * @api_private
 */
public class Base64OutputStreamFactory {

    private static Logger logger = Logger.getLogger(Base64OutputStreamFactory.class.getCanonicalName());

    public static OutputStream get(OutputStream os) {
        try {
            if (Misc.isRunningOnAndroid()) {
                Class c = Class.forName("android.util.Base64OutputStream");
                Constructor ctor = c.getDeclaredConstructor(OutputStream.class, int.class);
                // 2 = android.util.BASE64.NO_WRAP http://developer.android.com/reference/android/util/Base64.html#NO_WRAP
                return (OutputStream)ctor.newInstance(os, 2);
            } else {
                Class c = Class.forName("org.apache.commons.codec.binary.Base64OutputStream");
                Constructor ctor = c.getDeclaredConstructor(OutputStream.class, boolean.class, int.class, byte[].class);
                return (OutputStream) ctor.newInstance(os, true, 0, null);
            }
        } catch (RuntimeException e){
            throw e;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to load Base64OutputStream implementation", e);
            return null;
        }
    }
}
