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

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.logging.Logger;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import android.content.SharedPreferences;

import com.cloudant.sync.documentstore.encryption.EncryptionKey;

/**
 * Use this class to generate a Data Protection Key (DPK), i.e. a strong password that can be used
 * later on for other purposes like encrypting a database.
 *
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
 * - Return the DPK and save the encrypted version to the {@link SharedPreferences}.
 */
class KeyManager {
    private static final int BYTES_TO_BITS = 8;

    static final int ENCRYPTION_KEYCHAIN_AES_KEY_SIZE = 32;
    static final int ENCRYPTION_KEYCHAIN_ENCRYPTIONKEY_SIZE = 32;
    static final int ENCRYPTION_KEYCHAIN_PBKDF2_SALT_SIZE = 32;
    static final int ENCRYPTION_KEYCHAIN_PBKDF2_ITERATIONS = 10000;
    static final String ENCRYPTION_KEYCHAIN_VERSION = "1.0";
    static final int ENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE = 16;

    private static final Logger LOGGER = Logger.getLogger(KeyManager.class.getCanonicalName());
    private KeyStorage storage;
    private SecureRandom secureRandom;

    /**
     * Initialise a manager with a CDTEncryptionKeychainStorage instance.
     *
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
            this.secureRandom = new SecureRandom();
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
        if (password == null || password.equals("")) {
            throw new IllegalArgumentException("password is required to be a non-null/non-empty " +
                    "string");
        }
        KeyData data = this.storage.getEncryptionKeyData();
        if (data == null || !validateEncryptionKeyData(data)) {
            return null;
        }

        EncryptionKey dpk = null;
        SecretKey aesKey = null;
        try {
            aesKey = pbkdf2DerivedKeyForPassword(password, data.getSalt(), data.iterations,
                    ENCRYPTION_KEYCHAIN_AES_KEY_SIZE);
            byte[] dpkBytes = DPKEncryptionUtil.decryptAES(aesKey, data.getIv(), data
                    .getEncryptedDPK());
            dpk = new EncryptionKey(dpkBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new DPKException("Failed to decrypt DPK", e);
        } catch (InvalidKeySpecException e) {
            throw new DPKException("Failed to decrypt DPK", e);
        } catch (IllegalBlockSizeException e) {
            throw new DPKException("Failed to decrypt DPK", e);
        } catch (InvalidKeyException e) {
            throw new DPKException("Failed to decrypt DPK", e);
        } catch (BadPaddingException e) {
            throw new DPKException("Failed to decrypt DPK", e);
        } catch (InvalidAlgorithmParameterException e) {
            throw new DPKException("Failed to decrypt DPK", e);
        } catch (NoSuchPaddingException e) {
            throw new DPKException("Failed to decrypt DPK", e);
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
        if (password == null || password.equals("")) {
            throw new IllegalArgumentException("password is required to be a non-null/non-empty " +
                    "string");
        }
        try {
            if (!keyExists()) {
                byte[] dpkBytes = generateSecureRandomBytesWithLength
                        (ENCRYPTION_KEYCHAIN_ENCRYPTIONKEY_SIZE);
                byte[] salt = generateSecureRandomBytesWithLength
                        (ENCRYPTION_KEYCHAIN_PBKDF2_SALT_SIZE);
                byte[] iv = generateSecureRandomBytesWithLength
                        (ENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE);
                SecretKey aesKey = pbkdf2DerivedKeyForPassword(password, salt,
                        ENCRYPTION_KEYCHAIN_PBKDF2_ITERATIONS,
                        ENCRYPTION_KEYCHAIN_AES_KEY_SIZE);

                byte[] encryptedDpkBytes = DPKEncryptionUtil.encryptAES(aesKey, iv, dpkBytes);

                KeyData keyData = new KeyData(encryptedDpkBytes, salt, iv,
                        ENCRYPTION_KEYCHAIN_PBKDF2_ITERATIONS, ENCRYPTION_KEYCHAIN_VERSION);

                if (this.storage.saveEncryptionKeyData(keyData)) {
                    dpk = new EncryptionKey(dpkBytes);
                }
            }
        } catch (InvalidKeyException e) {
            throw new DPKException("Failed to encrypt DPK.  Cause: " + e.getLocalizedMessage(), e);
        } catch (NoSuchPaddingException e) {
            throw new DPKException("Failed to encrypt DPK.  Cause: " + e.getLocalizedMessage(), e);
        } catch (NoSuchAlgorithmException e) {
            throw new DPKException("Failed to encrypt DPK.  Cause: " + e.getLocalizedMessage(), e);
        } catch (IllegalBlockSizeException e) {
            throw new DPKException("Failed to encrypt DPK.  Cause: " + e.getLocalizedMessage(), e);
        } catch (BadPaddingException e) {
            throw new DPKException("Failed to encrypt DPK.  Cause: " + e.getLocalizedMessage(), e);
        } catch (InvalidAlgorithmParameterException e) {
            throw new DPKException("Failed to encrypt DPK.  Cause: " + e.getLocalizedMessage(), e);
        } catch (InvalidKeySpecException e) {
            throw new DPKException("Failed to encrypt DPK.  Cause: " + e.getLocalizedMessage(), e);
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
        if (data.getIv().length != ENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE) {
            LOGGER.warning("IV does not have the expected size: " +
                    ENCRYPTIONKEYCHAINMANAGER_AES_IV_SIZE + " bytes");
            return false;
        }
        return true;
    }

    private SecretKey pbkdf2DerivedKeyForPassword(String password, byte[] salt, int
            iterations, int length) throws NoSuchAlgorithmException, InvalidKeySpecException {
        if (length < 1) {
            throw new IllegalArgumentException("length must greater than 0");
        }

        if (password == null || password.length() < 1) {
            throw new IllegalArgumentException("password must not be null or empty String");
        }

        if (salt == null || salt.length < 1) {
            throw new IllegalArgumentException("salt must not be null or empty byte array");
        }

        if (iterations < 1) {
            throw new IllegalArgumentException("iterations must greater than 0");
        }

        SecretKeyFactory pbkdf2Factory;

        // Handle Android 4.4 changes to SecretKeyFactory API.
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
        secureRandom.nextBytes(randBytes);
        return randBytes;
    }
}
