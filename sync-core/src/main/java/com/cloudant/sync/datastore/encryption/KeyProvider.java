/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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
 * This class retrieves the user's SQLCipher password.
 * A secure key is generated based on the password, and
 * is then stored into local storage.
 *
 * TODO: Add JSONStore implementation for proper encryption and key management.
 * Created by estebanmlaver.
 */
public class KeyProvider  {

    private String encryptedKey;

    public KeyProvider(String password) {
        this.encryptedKey = password;
    }

    public String getEncryptionKey() {
        return encryptedKey;
    }
}
