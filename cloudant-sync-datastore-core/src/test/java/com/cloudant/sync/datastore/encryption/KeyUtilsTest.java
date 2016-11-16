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

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudant.sync.documentstore.encryption.EncryptionKey;
import com.cloudant.sync.documentstore.encryption.KeyProvider;
import com.cloudant.sync.internal.documentstore.encryption.KeyUtils;

public class KeyUtilsTest {

    @Test
    public void sqlCipherKeyForKeyProvider_nullProvider() {
        Assert.assertNull(KeyUtils.sqlCipherKeyForKeyProvider(null));
    }

    @Test
    public void sqlCipherKeyForKeyProvider_nullKey() {
        Assert.assertNull(KeyUtils.sqlCipherKeyForKeyProvider(new KeyProvider() {
            @Override
            public EncryptionKey getEncryptionKey() {
                return null;
            }
        }));
    }

    @Test
    public void sqlCipherKeyForKeyProvider_nullKeyBytes() {
        final EncryptionKey key = mock(EncryptionKey.class);
        when(key.getKey()).thenReturn(null);

        Assert.assertNull(KeyUtils.sqlCipherKeyForKeyProvider(new KeyProvider() {
            @Override
            public EncryptionKey getEncryptionKey() {
                return key;
            }
        }));
    }

    @Test
    public void sqlCipherKeyForKeyProvider_encodesBytesCorrectly() {
        byte[] keyBytes = new byte[] { -123, 53, -22, -15 };
        String expected = "x'8535eaf1'";

        final EncryptionKey key = mock(EncryptionKey.class);
        when(key.getKey()).thenReturn(keyBytes);

        Assert.assertEquals("Wrong hex output", expected,
                KeyUtils.sqlCipherKeyForKeyProvider(new KeyProvider() {
                    @Override
                    public EncryptionKey getEncryptionKey() {
                        return key;
                    }
                }));
    }

    @Test
    public void sqlCipherKeyForKeyProvider_encodes32BytesCorrectly() {
        byte[] keyBytes = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, -123, 53, -22, -15,
                -123, 53, -22, -15, -123, 53, -22, -15, -123, 53, -22, -15, -123, 53, -22, -15,
                -123, 53, -22, -15 };
        String expected = "x'8535eaf18535eaf18535eaf18535eaf18535eaf18535eaf18535eaf18535eaf1'";

        final EncryptionKey key = mock(EncryptionKey.class);
        when(key.getKey()).thenReturn(keyBytes);

        Assert.assertEquals("Wrong hex output", expected,
                KeyUtils.sqlCipherKeyForKeyProvider(new KeyProvider() {
                    @Override
                    public EncryptionKey getEncryptionKey() {
                        return key;
                    }
                }));
    }

}
