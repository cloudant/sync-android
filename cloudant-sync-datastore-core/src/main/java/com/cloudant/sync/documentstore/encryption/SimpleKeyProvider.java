/**
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

/**
 * SimpleKeyProvider simply takes raw key bytes in its
 * constructor and uses these to provide that key to
 * datastore methods.
 *
 * @api_public
 */
public class SimpleKeyProvider implements KeyProvider {

    private EncryptionKey key;

    public SimpleKeyProvider(byte[] key) {
        this.key = new EncryptionKey(key);
    }

    @Override
    public EncryptionKey getEncryptionKey() {
        return key;
    }
}
