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

/**
 * Classes implementing this interface provide encryption
 * keys used by the datastore when encryption is enabled.
 *
 * Created by Mike Rhodes on 11/05/15.
 *
 * @api_public
 */
public interface KeyProvider {

    /**
     * @return the encryption key used to encrypt datastore data
     */
    EncryptionKey getEncryptionKey();
}
