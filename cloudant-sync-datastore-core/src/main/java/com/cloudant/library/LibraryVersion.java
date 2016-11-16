/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.library;

import com.cloudant.sync.internal.util.Misc;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.util.Properties;

/**
 * Provides a HTTP User-Agent string for CloudantSync
 * @api_private
 */
public class LibraryVersion implements com.cloudant.http.Version {

    private static final String USER_AGENT;

    static {
        USER_AGENT = getUserAgent();
    }


    private static String getUserAgentFromResource() {
        final String defaultUserAgent = "CloudantSync";
        InputStream propertiesInputStream = null;
        final Properties properties = new Properties();
        try {
            propertiesInputStream = LibraryVersion.class.getClassLoader().getResourceAsStream
                    ("mazha.properties");
            properties.load(propertiesInputStream);
            return properties.getProperty("user.agent", defaultUserAgent);
        } catch (Exception ex) {
            return defaultUserAgent;
        } finally {
            IOUtils.closeQuietly(propertiesInputStream);
        }
    }

    private static String getUserAgent() {
        String userAgent;
        String ua = getUserAgentFromResource();
        if (Misc.isRunningOnAndroid()) {
            try {
                Class c = Class.forName("android.os.Build$VERSION");
                String codename = (String) c.getField("CODENAME").get(null);
                int sdkInt = c.getField("SDK_INT").getInt(null);
                userAgent = String.format("%s Android %s %d", ua, codename, sdkInt);
            } catch (Exception e) {
                userAgent = String.format("%s Android unknown version", ua);
            }
        } else {
            userAgent = String.format("%s Java (%s; %s; %s)",
                    ua,
                    System.getProperty("os.arch"),
                    System.getProperty("os.name"),
                    System.getProperty("os.version"));
        }
        return userAgent;
    }


    @Override
    public String getUserAgentString() {
        return USER_AGENT;
    }
}
