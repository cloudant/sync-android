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

package com.cloudant.sync.datastore.encryption;

import android.content.Context;

import com.cloudant.sync.internal.util.Misc;

import java.lang.reflect.InvocationTargetException;
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
            if (Misc.isRunningOnAndroid()) {

                try {
                    Class klass = Class.forName("android.support.test.InstrumentationRegistry");
                    Method method = klass.getMethod("getTargetContext");
                    return (Context) method.invoke(null);
                } catch (ClassNotFoundException e){

                } catch (NoSuchMethodException e){

                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }


            }
        return context;
    }
}
