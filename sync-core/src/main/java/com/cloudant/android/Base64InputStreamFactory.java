package com.cloudant.android;

import com.cloudant.sync.util.Misc;

import java.io.InputStream;
import java.lang.reflect.Constructor;

/**
 * Created by tomblench on 07/07/2014.
 */
public class Base64InputStreamFactory {
    public static InputStream get(InputStream is) {
        try {
            if (Misc.isRunningOnAndroid()) {
                Class c = Class.forName("android.util.Base64InputStream");
                Constructor ctor = c.getDeclaredConstructor(InputStream.class, int.class);
                return (InputStream)ctor.newInstance(is, 0);
            } else {
                Class c = Class.forName("org.apache.commons.codec.binary.Base64InputStream");
                Constructor ctor = c.getDeclaredConstructor(InputStream.class);
                return (InputStream)ctor.newInstance(is);
            }
        } catch (Exception e) {
            // TODO log
            return null;
        }
    }
}
