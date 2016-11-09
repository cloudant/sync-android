package com.cloudant.sync.datastore.encryption;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

import com.cloudant.sync.documentstore.encryption.CachingKeyProvider;
import com.cloudant.sync.documentstore.encryption.EncryptionKey;
import com.cloudant.sync.documentstore.encryption.KeyProvider;
import com.cloudant.sync.documentstore.encryption.SimpleKeyProvider;

public class CachingKeyProviderTests {
    @Before
    public void beforeMethod() {
        assumeNotNull(ProviderTestUtil.getContext());
    }

    @Test
    public void testWithSimpleKeyProvider() {
        // Generate 32-byte key
        byte[] keyBytes = new RandomKeyProvider().getEncryptionKey().getKey();

        // Create provider
        CachingKeyProvider provider = new CachingKeyProvider(new SimpleKeyProvider(keyBytes));

        // Get the key
        EncryptionKey encryptionKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not created", encryptionKey);

        // Assert value is same as original key
        assertTrue("encryptionKey bytes != keyBytes", Arrays.equals(encryptionKey.getKey(),
                keyBytes));

        // Get the cached key
        EncryptionKey cachedKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not cached", cachedKey);

        // Assert cached value is same as original key
        assertTrue("encryptionKey bytes != keyBytes", Arrays.equals(encryptionKey.getKey(),
                keyBytes));

        // Assert cached key and key are the same object reference
        assertEquals("cachedKey did not match createdKey", encryptionKey,
                cachedKey);
    }

    @Test
    public void testWithAndroidKeyProvider() {
        CachingKeyProvider provider = new CachingKeyProvider(new AndroidKeyProvider(ProviderTestUtil
                .getContext(),
                ProviderTestUtil.password, ProviderTestUtil.getUniqueIdentifier()));

        // This will cause the provider to generate a key using the AndroidKeyProvider. The key
        // will be encrypted, persisted, and stored in memory.
        EncryptionKey createdKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not created", createdKey);

        // This will cause the provider to load the cached key.
        EncryptionKey cachedKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not cached", cachedKey);

        // Assert cached key and created key are the same object reference
        assertEquals("cachedKey did not match createdKey", createdKey,
                cachedKey);

        AndroidKeyProvider androidKeyProvider = (AndroidKeyProvider) provider.getKeyProvider();
        androidKeyProvider.getManager().clearKey();
    }

    @Test
    public void testTwoProvidersSameKey() {
        // Generate 32-byte key
        byte[] keyBytes = new RandomKeyProvider().getEncryptionKey().getKey();

        // Create providers
        CachingKeyProvider provider1 = new CachingKeyProvider(new SimpleKeyProvider(keyBytes));
        CachingKeyProvider provider2 = new CachingKeyProvider(new SimpleKeyProvider(keyBytes));

        // Get provider1 key
        EncryptionKey encryptionKey1 = provider1.getEncryptionKey();
        assertNotNull("encryptionKey1 should not be null", encryptionKey1);

        // Assert value is same as original key
        assertTrue("encryptionKey1 bytes != keyBytes", Arrays.equals(encryptionKey1.getKey(),
                keyBytes));

        // Get provider2 key
        EncryptionKey encryptionKey2 = provider2.getEncryptionKey();
        assertNotNull("encryptionKey2 should not be null", encryptionKey2);

        // Assert value is same as original key
        assertTrue("encryptionKey2 bytes != keyBytes", Arrays.equals(encryptionKey2.getKey(),
                keyBytes));

        // Assert encryptionKey1 value is same as encryptionKey2 value
        assertTrue("encryptionKey bytes != keyBytes", Arrays.equals(encryptionKey1.getKey(),
                encryptionKey2.getKey()));

        // Assert encryptionKey1 and encryptionKey2 are not the same object reference
        assertNotEquals("cachedKey did not match createdKey", encryptionKey1,
                encryptionKey2);
    }

    @Test
    public void testTwoProvidersDifferentKey() {
        // Generate unique 32-byte keys
        byte[] keyBytes1 = new RandomKeyProvider().getEncryptionKey().getKey();
        byte[] keyBytes2 = new RandomKeyProvider().getEncryptionKey().getKey();

        // Create providers
        CachingKeyProvider provider1 = new CachingKeyProvider(new SimpleKeyProvider(keyBytes1));
        CachingKeyProvider provider2 = new CachingKeyProvider(new SimpleKeyProvider(keyBytes2));

        // Get provider1 key
        EncryptionKey encryptionKey1 = provider1.getEncryptionKey();
        assertNotNull("encryptionKey1 should not be null", encryptionKey1);

        // Assert value is same as keyBytes1
        assertTrue("encryptionKey1 bytes != keyBytes", Arrays.equals(encryptionKey1.getKey(),
                keyBytes1));

        // Get provider2 key
        EncryptionKey encryptionKey2 = provider2.getEncryptionKey();
        assertNotNull("encryptionKey2 should not be null", encryptionKey2);

        // Assert value is same as keyBytes2
        assertTrue("encryptionKey2 bytes != keyBytes", Arrays.equals(encryptionKey2.getKey(),
                keyBytes2));

        // Assert encryptionKey1 value is not the same as encryptionKey2 value
        assertFalse("encryptionKey bytes != keyBytes", Arrays.equals(encryptionKey1.getKey(),
                encryptionKey2.getKey()));
    }

    // Negative tests
    @Test(expected = IllegalArgumentException.class)
    public void testCreateProviderWithNullKeyProvider() {
        new CachingKeyProvider(null);
        fail("CachingKeyProvider constructor should fail if context is null");
    }

    @Test
    public void testKeyCached() {
        CachingKeyProvider provider = new CachingKeyProvider(new RandomKeyProvider());

        EncryptionKey encryptionKey = provider.getEncryptionKey();
        EncryptionKey encryptionKey1 = provider.getEncryptionKey();

        assertEquals("encryptionKey and encryptionKey1 should have the same object reference",
                encryptionKey, encryptionKey1);
    }

    private class RandomKeyProvider implements KeyProvider {
        private Random r = new Random();

        public EncryptionKey getEncryptionKey() {
            byte[] buf = new byte[32];
            r.nextBytes(buf);
            return new EncryptionKey(buf);
        }
    }
}
