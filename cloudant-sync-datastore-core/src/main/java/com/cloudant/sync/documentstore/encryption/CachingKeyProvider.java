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

package com.cloudant.sync.documentstore.encryption;

import java.util.logging.Logger;

/**
 * Given a user-provided {@link KeyProvider}, it provides an in-memory cache for retrieving an
 * {@link EncryptionKey}. This improves performance when multiple encrypted datastores are used.
 *
 * This class implements the interface {@link KeyProvider} and it can be used to create an
 * encrypted datastore.
 *
 * @see KeyProvider
 */
public class CachingKeyProvider implements KeyProvider {

    private static final Logger LOGGER = Logger.getLogger(CachingKeyProvider.class
            .getCanonicalName());
    private final KeyProvider keyProvider;
    private EncryptionKey encryptionKey;

    /**
     * Creates a {@link CachingKeyProvider} containing a {@link KeyProvider} whose {@link
     * EncryptionKey} can be cached
     *
     * @param keyProvider The {@link KeyProvider} to use for encrypting a datastore
     */
    public CachingKeyProvider(KeyProvider keyProvider) {
        if (keyProvider == null) {
            LOGGER.severe("All parameters are mandatory");
            throw new IllegalArgumentException("All parameters are mandatory");
        }

        this.keyProvider = keyProvider;
    }

    @Override
    public synchronized EncryptionKey getEncryptionKey() {
        if (encryptionKey == null) {
            encryptionKey = keyProvider.getEncryptionKey();
        }

        return encryptionKey;
    }

    /**
     * @return the {@link KeyProvider} used for creating and retrieving the {@link EncryptionKey}
     */
    public KeyProvider getKeyProvider() {
        return keyProvider;
    }
}
