/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Locale;
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
        return javaRuntime.toLowerCase(Locale.ENGLISH).contains(ANDROID_RUNTIME);
    }

    public static byte[] getSha1(InputStream shaFis) throws IOException {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
            int bufSiz = 1024;
            byte buf[] = new byte[bufSiz];
            int bytesRead;
            while ((bytesRead = shaFis.read(buf)) != -1) {
                sha1.update(buf, 0, bytesRead);
            }
        } catch (NoSuchAlgorithmException e){
            throw new IllegalStateException(e);
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
    /**
     * Check that a parameter is not null and throw NullPointerException with a message of
     * errorMessagePrefix + " must not be null." if it is null, defaulting to "Parameter must not be
     * null.".
     *
     * @param param              the parameter to check
     * @param errorMessagePrefix the prefix of the error message to use for the
     *                           IllegalArgumentException if the parameter was null
     * @throws NullPointerException if the arg is null.
     */
    public static void checkNotNull(Object param, String errorMessagePrefix) throws
            NullPointerException {
        if (param == null) {
            throw new NullPointerException((errorMessagePrefix != null ? errorMessagePrefix :
                    "Parameter") + " must not be null.");
        }
    }

    /**
     * Check that a string parameter is not null or empty and throw IllegalArgumentException with a
     * message of errorMessagePrefix + " must not be empty." if it is empty, defaulting to
     * "Parameter must not be empty".
     *
     * @param param              the string to check
     * @param errorMessagePrefix the prefix of the error message to use for the
     *                           IllegalArgumentException if the parameter was null or empty
     * @throws IllegalArgumentException if the string is null or empty
     */
    public static void checkNotNullOrEmpty(String param, String errorMessagePrefix) throws
            IllegalArgumentException {
        checkArgument(!isStringNullOrEmpty(param), (errorMessagePrefix != null ? errorMessagePrefix :
              "Parameter") + " must not be " + (param == null ? "null." : "empty."));
    }

    /**
     * @param param the string to check
     * @return true if the string is null or emtpy
     */
    public static boolean isStringNullOrEmpty(String param) {
        if (param == null || param.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check an argument meets a condition and throw an IllegalArgumentException if it doesn't.
     *
     * @param argCondition the condition to check
     * @param errorMessage the error message to use in the IllegalArgumentException
     * @throws IllegalArgumentException if the argCondition is false
     */
    public static void checkArgument(boolean argCondition, String errorMessage) throws
            IllegalArgumentException {
        if (!argCondition) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Check if a condition is true and throw an IllegalStateException with the given
     * errorMessage if it is not.
     *
     * @param stateCheck   the state condition to check
     * @param errorMessage the error string to add to the IllegalStateException
     * @throws IllegalStateException if the stateCheck is false
     */
    public static void checkState(boolean stateCheck, String errorMessage) throws
            IllegalStateException {
        if (!stateCheck) {
            throw new IllegalStateException(errorMessage);
        }
    }
}
