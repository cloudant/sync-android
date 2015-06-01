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
}
