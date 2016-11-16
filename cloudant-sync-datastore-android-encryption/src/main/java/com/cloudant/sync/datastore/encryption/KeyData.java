/*
 * Copyright © 2015 IBM Corp. All rights reserved.
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

/**
 * This class defines all the values required to store an encrypted DPK and decipher it later on.
 *
 * @see KeyStorage
 */
class KeyData {

    private final byte[] encryptedDPK;
    private final byte[] salt;
    private final byte[] iv;
    public final int iterations;
    public final String version;

    /**
     * Contains the encrypted DPK and all the values required to decrypt it.
     *
     * @param encryptedDPK The encrypted Data Protection Key (DPK)
     * @param salt         The salt used for encryption
     * @param iv           The initialization vector
     * @param iterations   The number of iterations
     * @param version      The version
     */
    public KeyData(byte[] encryptedDPK, byte[] salt, byte[] iv, int iterations, String version) {
        if (encryptedDPK != null && salt != null && iv != null && iterations > 0 && version !=
                null && encryptedDPK.length > 0 && salt.length > 0 && iv.length > 0 && !version
                .equals("")) {
            this.encryptedDPK = encryptedDPK;
            this.salt = salt;
            this.iv = iv;
            this.iterations = iterations;
            this.version = version;
        } else {
            throw new IllegalArgumentException("All parameters are required to be " +
                    "non-null/non-empty values");
        }
    }

    /**
     * @return A byte array containing the encrypted DPK
     */
    public byte[] getEncryptedDPK() {
        return encryptedDPK;
    }

    /**
     * @return A byte array containing the salt
     */
    public byte[] getSalt() {
        return salt;
    }

    /**
     * @return A byte array containing the iv
     */
    public byte[] getIv() {
        return iv;
    }
}
