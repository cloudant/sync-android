package com.cloudant.sync.datastore.encryption;

import android.content.Context;

import com.cloudant.sync.util.Misc;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

public class ProviderTestUtil {
    public static final String password = "provider1password";
    private static Context context = null;

    public static String getUniqueIdentifier() {
        return "test-id-" + UUID.randomUUID();
    }

    public static KeyData createKeyData() {
        byte[] encryptedDPK = new byte[KeyManager.ENCRYPTION_KEYCHAIN_AES_KEY_SIZE];
        byte[] salt = new byte[KeyManager.ENCRYPTION_KEYCHAIN_PBKDF2_SALT_SIZE];
        byte[] iv = new byte[KeyManager.ENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE];
        int iterations = new Random().nextInt(100000);
        String version = "1." + System.currentTimeMillis();

        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(encryptedDPK);
        secureRandom.nextBytes(salt);
        secureRandom.nextBytes(iv);

        return new KeyData(encryptedDPK, salt, iv, iterations, version);
    }

    public static synchronized Context getContext(){
        if(context == null) {
            if (Misc.isRunningOnAndroid()) {
                try {
                    Class<?> androidTestUtilClazz = Class.forName("cloudant.com.androidtest.AndroidTestUtil");
                    if (androidTestUtilClazz != null) {
                        Field contextField = androidTestUtilClazz.getField("context");
                        if (contextField != null) {
                            Object contextObj = contextField.get(null);
                            if (contextObj instanceof Context)
                                context = (Context) contextObj;
                        }

                    }

                } catch (Exception e) {
                    // do nothing
                    context = null;
                }
            }
        }
        return context;
    }
}
