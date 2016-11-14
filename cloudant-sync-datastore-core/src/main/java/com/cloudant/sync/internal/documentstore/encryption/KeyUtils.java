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

package com.cloudant.sync.internal.documentstore.encryption;

import com.cloudant.sync.documentstore.encryption.EncryptionKey;
import com.cloudant.sync.documentstore.encryption.KeyProvider;

import org.apache.commons.codec.binary.Hex;

/**
 * Utility methods for manipulating keys.
 *
 * @api_private
 */
public class KeyUtils {

    /**
     * Take a KeyProvider and return a hex-encoded string version
     * of the key suitable for passing directly to SQLCipher
     * (i.e., including `x'` prefix and `'` suffix).
     *
     * @param provider KeyProvider to encode key for
     * @return Hex-encoded SQLCipher-compatible key string
     */
    public static String sqlCipherKeyForKeyProvider(KeyProvider provider) {
        // We get a byte[] from the EncryptionKey object passed by the provider.
        // This needs to be made into a hex string and wrapped in `x'keyhex'`
        // before passing to SQLCipher's open method.
        if (provider == null) {
            return null;
        }
        EncryptionKey key = provider.getEncryptionKey();
        if (key == null) {
            return null;
        }
        byte[] keyBytes = key.getKey();
        if (keyBytes == null) {
            return null;
        }

        String hexKey = new String(Hex.encodeHex(keyBytes));
        return String.format("x'%1$s'", hexKey);
    }
}
