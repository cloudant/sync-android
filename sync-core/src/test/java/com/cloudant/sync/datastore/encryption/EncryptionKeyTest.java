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

import org.junit.Assert;
import org.junit.Test;

/**
 * This test makes sure that the EncryptionKey class only allows
 * itself to be constructed using a 32 byte array.
 */
public class EncryptionKeyTest {

    byte[] keyLength32 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53, -22 };

    byte[] keyLength31 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53 };

    byte[] keyLength33 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53, -22, -123 };

    byte[] keyLength16 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22 };

    @Test
    public void keyWith32ByteLength() {
        EncryptionKey key = new EncryptionKey(keyLength32);
        Assert.assertEquals("Returned key should match creation key", keyLength32, key.getKey());
    }

    @Test(expected = IllegalArgumentException.class)
    public void keyWith31ByteLength() {
        new EncryptionKey(keyLength31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void keyWith33ByteLength() {
        new EncryptionKey(keyLength33);
    }

    @Test(expected = IllegalArgumentException.class)
    public void keyWith16ByteLength() {
        new EncryptionKey(keyLength16);
    }

    @Test(expected = IllegalArgumentException.class)
    public void keyWithNullBytes() {
        new EncryptionKey(null);
    }
}
