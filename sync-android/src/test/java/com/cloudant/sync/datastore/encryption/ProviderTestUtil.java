package com.cloudant.sync.datastore.encryption;

import com.cloudant.sync.datastore.encryption.KeyData;

import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

public class ProviderTestUtil {
    public static final String password = "provider1password";

    public static String getUniqueIdentifier() {
        return "test-id-" + UUID.randomUUID();
    }

    public static KeyData createKeyData() {
        byte[] encryptedDPK = new byte[32];
        byte[] salt = new byte[32];
        byte[] iv = new byte[32];
        int iterations = new Random().nextInt(100000);
        String version = "1." + System.currentTimeMillis();

        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(encryptedDPK);
        secureRandom.nextBytes(salt);
        secureRandom.nextBytes(iv);

        return new KeyData(encryptedDPK, salt, iv, iterations, version);
    }
}
