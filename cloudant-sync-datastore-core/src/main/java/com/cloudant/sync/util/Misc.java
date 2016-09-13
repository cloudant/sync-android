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

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Internal utility class
 * @api_private
 */
public class Misc {

    public static final String ANDROID_RUNTIME = "android runtime";
    private static final Logger logger = Logger.getLogger(Misc.class.getCanonicalName());

    public static String createUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static boolean isRunningOnAndroid() {
        String javaRuntime = System.getProperty("java.runtime.name", "");
        return javaRuntime.toLowerCase().contains(ANDROID_RUNTIME);
    }

    public static byte[] getSha1(InputStream shaFis) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
            int bufSiz = 1024;
            byte buf[] = new byte[bufSiz];
            int bytesRead;
            while ((bytesRead = shaFis.read(buf)) != -1) {
                sha1.update(buf, 0, bytesRead);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,"Problem calculating SHA1 for stream",e);
            return null;
        }
        return sha1.digest();
    }

    /**
     * Utility to join strings with a separator. Skips null strings and does not append a trailing
     * separator.
     *
     * @param separator     the string to use to separate the entries
     * @param stringsToJoin the strings to join together
     * @return the joined string
     */
    public static String join(String separator, Collection<String> stringsToJoin) {
        Iterator<String> iterator = stringsToJoin.iterator();
        StringBuilder builder = new StringBuilder();
        // Check if there is at least 1 element then use do/while to avoid trailing separator
        int index = stringsToJoin.size();
        for (String str : stringsToJoin) {
            index--;
            if (str != null) {
                builder.append(str);
                if (index > 0) {
                    builder.append(separator);
                }
            }
        }
        return builder.toString();
    }
}
