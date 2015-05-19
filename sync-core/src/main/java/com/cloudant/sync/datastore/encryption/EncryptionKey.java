/**
 * Copyright (c) 2015 IBM Cloudant, Inc. All rights reserved.
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

import java.util.Arrays;

/**
 * Class to enforce restrictions on encryption keys used
 * with the datastore.
 *
 * SQLCipher requires a 32-byte/256-bit key. This class
 * enforces that.
 */
public class EncryptionKey {

    private final static int REQUIRED_KEY_LENGTH = 32;

    private final byte[] key;

    public EncryptionKey(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Key array must not be null");
        }

        if (key.length != REQUIRED_KEY_LENGTH) {
            throw new IllegalArgumentException("Key array must be 32 bytes");
        }

        this.key = Arrays.copyOf(key, REQUIRED_KEY_LENGTH);
    }

    public byte[] getKey() {
        return Arrays.copyOf(key, REQUIRED_KEY_LENGTH);
    }

}
