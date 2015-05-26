/**
 * Copyright (c) 2015 IBM Cloudant, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore.encryption;

import android.os.Build;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import android.content.SharedPreferences;

/**
 * Use this class to generate a Data Protection Key (DPK), i.e. a strong password that can be used
 * later on for other purposes like encrypting a database.
 * <p/>
 * The generated DPK is automatically encrypted and saved to the {@link android.content
 * .SharedPreferences}, for this reason, it is
 * necessary to provide a password to generate and retrieve the DPK. On a high level, this is
 * done as
 * follow:
 * - Generate a DPK as a 32 bytes buffer with secure random values.
 * - Generate a salt as a 32 bytes buffer with secure random values.
 * - Use PBKDF2 to derive a key based on the user-provided password and the salt.
 * - Generate an initialization vector (IV) as a 16 bytes buffer with secure random values.
 * - Use AES to cipher the DPK with the key and the IV.
 * - Return the DPK and save the encrypted version to the keychain.
 */
class KeyManager {
    private static final int BYTES_TO_BITS = 8;

    private static final int CDTENCRYPTION_KEYCHAIN_AES_KEY_SIZE = 32;
    private static final int CDTENCRYPTION_KEYCHAIN_ENCRYPTIONKEY_SIZE = 32;
    private static final int CDTENCRYPTION_KEYCHAIN_PBKDF2_SALT_SIZE = 32;
    private static final int CDTENCRYPTION_KEYCHAIN_PBKDF2_ITERATIONS = 10000;
    private static final String CDTENCRYPTION_KEYCHAIN_VERSION = "1.0";
    private static final int CDTENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE = 16;

    private static final Logger LOGGER = Logger.getLogger(KeyManager.class.getCanonicalName());
    private KeyStorage storage;

    /**
     * Initialise a manager with a CDTEncryptionKeychainStorage instance.
     * <p/>
     * A {@link KeyStorage} binds an entry in the {@link SharedPreferences} to an identifier. The
     * data protection key (DPK) saved to the {@link SharedPreferences} by this class will
     * therefore be bound to the storage's identifier. To save different DPKs (say for different
     * users of your application), create multiple managers using storages initialised with
     * different identifiers.
     *
     * @param storage Storage instance to save DPKs to the {@link SharedPreferences}
     * @see KeyStorage
     */
    public KeyManager(KeyStorage storage) {
        if (storage != null) {
            this.storage = storage;
        } else {
            LOGGER.severe("Storage is mandatory");
            throw new IllegalArgumentException("Storage is mandatory");
        }
    }

    /**
     * Returns the decrypted Data Protection Key (DPK) from the {@link SharedPreferences}.
     *
     * @param password Password used to decrypt the DPK
     * @return The DPK
     */
    public EncryptionKey loadKeyUsingPassword(String password) {
        KeyData data = this.storage.getEncryptionKeyData();
        if (data == null || !validateEncryptionKeyData(data)) {
            return null;
        }

        EncryptionKey dpk = null;
        SecretKey aesKey = null;
        try {
            aesKey = pbkdf2DerivedKeyForPassword(password, data.getSalt(), data.getIterations(),
                    CDTENCRYPTION_KEYCHAIN_AES_KEY_SIZE);
            byte[] dpkBytes = DKPEncryptionUtil.decryptAES(aesKey, data.getIv(), data
                    .getEncryptedDPK());

            dpk = new EncryptionKey(dpkBytes);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to decrypt DPK: " + e.getLocalizedMessage(), e);
        }
        return dpk;
    }

    /**
     * Generates a Data Protection Key (DPK), encrypts it, and stores it inside the {@link
     * SharedPreferences}.
     *
     * @param password Password used to encrypt the DPK
     * @return The DPK
     */
    public EncryptionKey generateAndSaveKeyProtectedByPassword(String password) {
        EncryptionKey dpk = null;
        try {
            if (!keyExists()) {
                byte[] dpkBytes = generateSecureRandomBytesWithLength
                        (CDTENCRYPTION_KEYCHAIN_ENCRYPTIONKEY_SIZE);
                byte[] salt = generateSecureRandomBytesWithLength
                        (CDTENCRYPTION_KEYCHAIN_PBKDF2_SALT_SIZE);
                byte[] iv = generateSecureRandomBytesWithLength
                        (CDTENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE);
                SecretKey aesKey = pbkdf2DerivedKeyForPassword(password, salt,
                        CDTENCRYPTION_KEYCHAIN_PBKDF2_ITERATIONS,
                        CDTENCRYPTION_KEYCHAIN_AES_KEY_SIZE);

                byte[] encryptedDpkBytes = DKPEncryptionUtil.encryptAES(aesKey, iv, dpkBytes);

                KeyData keyData = new KeyData(encryptedDpkBytes, salt, iv,
                        CDTENCRYPTION_KEYCHAIN_PBKDF2_ITERATIONS, CDTENCRYPTION_KEYCHAIN_VERSION);

                if (this.storage.saveEncryptionKeyData(keyData)) {
                    dpk = new EncryptionKey(dpkBytes);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to encrypt DPK: " + e.getLocalizedMessage(), e);
        }
        return dpk;
    }

    /**
     * Checks if the encrypted Data Protection Key (DPK) is inside the {@link SharedPreferences}.
     *
     * @return true if the encrypted DPK is inside the {@link SharedPreferences}, false otherwise
     */
    public boolean keyExists() {
        return this.storage.encryptionKeyDataExists();
    }

    /**
     * Clears security metadata from the {@link SharedPreferences}.
     *
     * @return Success (true) or failure (false)
     */
    public boolean clearKey() {
        return this.storage.clearEncryptionKeyData();
    }

    // PRIVATE HELPER METHODS
    private boolean validateEncryptionKeyData(KeyData data) {
        if (data.getIv().length != CDTENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE) {
            LOGGER.warning("IV does not have the expected size: " +
                    CDTENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE + " bytes");
            return false;
        }
        return true;
    }

    private SecretKey pbkdf2DerivedKeyForPassword(String password, byte[] salt, int
            iterations, int length) throws NoSuchAlgorithmException, InvalidKeySpecException {
        if (length < 1) {
            throw new IllegalArgumentException("Length must greater than 0");
        }

        if (password == null || password.length() < 1) {
            throw new IllegalArgumentException("Length must greater than 0");
        }

        if (salt == null || salt.length < 1) {
            throw new IllegalArgumentException("Length must greater than 0");
        }

        if (iterations < 1) {
            throw new IllegalArgumentException("Length must greater than 0");
        }

        SecretKeyFactory pbkdf2Factory;
        if (Build.VERSION.SDK_INT >= 19) {
            // Use compatibility key factory required for backwards compatibility in API 19 and up.
            pbkdf2Factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1And8bit"); //$NON-NLS-1$
        } else {
            // Traditional key factory.
            pbkdf2Factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1"); //$NON-NLS-1$
        }

        PBEKeySpec keySpec = new PBEKeySpec(password.toCharArray(), salt, iterations, length *
                BYTES_TO_BITS); //$NON-NLS-1$
        return pbkdf2Factory.generateSecret(keySpec);
    }

    private byte[] generateSecureRandomBytesWithLength(int length) {
        byte[] randBytes = new byte[length];
        new SecureRandom().nextBytes(randBytes);
        return randBytes;
    }
}
