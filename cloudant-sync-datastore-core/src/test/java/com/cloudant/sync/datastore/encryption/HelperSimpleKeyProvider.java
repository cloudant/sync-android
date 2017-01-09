/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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
package com.cloudant.sync.datastore.encryption;

import com.cloudant.sync.documentstore.encryption.SimpleKeyProvider;

/**
 * Class provides SQLCipher key for test cases.
 * Created by estebanmlaver.
 */
public class HelperSimpleKeyProvider extends SimpleKeyProvider {

    public HelperSimpleKeyProvider() {
        // Create a key provider with a hard-coded key
        super(new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
                -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
                -15, -123, -22, -15, 53, -22 });
    }

}
