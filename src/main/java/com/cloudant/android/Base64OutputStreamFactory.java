package com.cloudant.android;

import com.cloudant.sync.util.Misc;

import java.io.OutputStream;

/**
 * Created by tomblench on 07/07/2014.
 */
public class Base64OutputStreamFactory {

    // TODO load these with class.forName()
    public static OutputStream get(OutputStream os) {
        if (Misc.isRunningOnAndroid()) {
            return new android.util.Base64OutputStream(os, 0);
        } else {
            return new org.apache.commons.codec.binary.Base64OutputStream(os, true, 0, null);
        }
    }
}
