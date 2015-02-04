package com.cloudant.common;

import com.cloudant.mazha.CouchConfig;
import com.cloudant.mazha.SpecifiedCouch;
import com.cloudant.sync.util.Misc;
import com.google.common.base.Strings;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by tomblench on 20/01/15.
 */
public abstract class CouchTestBase {

    private static final Boolean SPECIFIED_COUCH = Boolean.valueOf(
            System.getProperty("test.with.specified.couch",Boolean.FALSE.toString()));

    public static final Boolean IGNORE_COMPACTION = Boolean.valueOf(
            System.getProperty("test.couch.ignore.compaction",Boolean.FALSE.toString()));

    public static final Boolean IGNORE_AUTH_HEADERS = Boolean.valueOf(
            System.getProperty("test.couch.ignore.auth.headers",Boolean.FALSE.toString()));

    public CouchConfig getCouchConfig(String db) {

        if (SPECIFIED_COUCH) {
            return SpecifiedCouch.defaultConfig(db);
        }
        else {
            String host;
             // If we're running on the Android emulator, 127.0.0.1 is the emulated device, rather
             // than the host machine. Instead we connect to 10.0.2.2.
            if(Misc.isRunningOnAndroid()){
                host = "10.0.2.2";
            } else {
                host = "127.0.0.1";
            }
            return this.defaultConfig(host, db);
        }
    }

    public CouchConfig defaultConfig(String host, String databasePath) {
        try {
            // we use String.format rather than the multi-arg URI constructor to avoid database
            // names being (double) escaped
            String urlString = String.format("http://%s:5984/%s", host, databasePath);
            CouchConfig config = new CouchConfig(new URI(urlString));
            return config;
        } catch (URISyntaxException use) {
            use.printStackTrace();
            return null;
        }
    }
}
