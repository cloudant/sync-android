/**
 * Copyright © 2015 IBM Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 *
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore.encryption;

import android.content.Context;

import com.cloudant.sync.documentstore.encryption.EncryptionKey;
import com.cloudant.sync.documentstore.encryption.KeyProvider;

import java.util.logging.Logger;

/**
 * This class implements the interface {@link KeyProvider} and it can be used to create an
 * encrypted datastore.
 *
 * Given an user-provided password and an identifier, it generates a strong key and store it safely
 * in the application's {@link android.content.SharedPreferences}, so the same key can be retrieved
 * later provided that the user supplies the same password and id.
 *
 * The password is used to protect the key before saving it to the {@link android.content
 * .SharedPreferences}. The identifier is an
 * easy way to have more than one encryption key in the same app, the only condition is to provide
 * different ids for each of them.
 *
 * @see KeyProvider
 * @see KeyManager
 * @see KeyStorage
 *
 * @api_public
 */
public class AndroidKeyProvider implements KeyProvider {
    private static final Logger LOGGER = Logger.getLogger(AndroidKeyProvider.class
            .getCanonicalName());
    private String password;
    private KeyManager manager;


    /**
     * Creates a {@link AndroidKeyProvider} containing a {@link KeyManager} associated with the
     * provided identifier.
     *
     * @param context    The application's {@link Context}
     * @param password   An user-provided password
     * @param identifier The data saved in the {@link android.content.SharedPreferences} will be
     *                   accessed with this identifier
     * @see KeyManager
     */
    public AndroidKeyProvider(Context context, String password, String identifier) {
        if (context != null && password != null && identifier != null && !password.equals("") &&
                !identifier.equals("")) {
            this.password = password;
            KeyStorage storage = new KeyStorage(context, identifier);
            this.manager = new KeyManager(storage);
        } else {
            LOGGER.severe("All parameters are mandatory");
            throw new IllegalArgumentException("All parameters are mandatory");
        }
    }

    @Override
    public synchronized EncryptionKey getEncryptionKey() {
        EncryptionKey key = null;
        if (this.manager.keyExists()) {
            key = this.manager.loadKeyUsingPassword(this.password);
        } else {
            key = this.manager.generateAndSaveKeyProtectedByPassword(this.password);
        }

        return key;
    }

    KeyManager getManager() {
        return this.manager;
    }
}
