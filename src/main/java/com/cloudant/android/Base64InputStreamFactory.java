package com.cloudant.android;

import com.cloudant.sync.util.Misc;

import java.io.InputStream;

/**
 * Created by tomblench on 07/07/2014.
 */
public class Base64InputStreamFactory {
    // TODO load these with class.forName()
    public static InputStream get(InputStream is) {
        if (Misc.isRunningOnAndroid()) {
            return new android.util.Base64InputStream(is, 0);
        } else {
            return new org.apache.commons.codec.binary.Base64InputStream(is);
        }
    }
}
