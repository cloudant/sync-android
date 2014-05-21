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
