package com.cloudant.sync.datastore.encryption;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AndroidKeyProviderTests {

    @Test
    public void testCreateProviderWithIdentifier() {
        AndroidKeyProvider provider = new AndroidKeyProvider(ProviderTestUtil.getContext(),
                ProviderTestUtil.password, ProviderTestUtil.getUniqueIdentifier());

        // This will cause the provider to generate a key.  The key will be encrypted and persisted.
        EncryptionKey createdKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not created", createdKey);

        // This will cause the provider to decrypt and load a key that was persisted.
        EncryptionKey loadedKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not loaded", loadedKey);

        // Compare the decrypted persisted key to the original generated key.
        assertTrue("loadedKey did not match createdKey", Arrays.equals(createdKey.getKey(),
                loadedKey.getKey()));

        provider.getManager().clearKey();
    }

    @Test
    public void testCreateTwoProvidersWithSameIdentifier() {
        String identifier = ProviderTestUtil.getUniqueIdentifier();
        AndroidKeyProvider provider1 = new AndroidKeyProvider(ProviderTestUtil.getContext(),
                ProviderTestUtil.password, identifier);
        AndroidKeyProvider provider2 = new AndroidKeyProvider(ProviderTestUtil.getContext(),
                ProviderTestUtil.password, identifier);

        EncryptionKey provider1Key = provider1.getEncryptionKey();
        assertNotNull("provider1 EncryptionKey was not created", provider1Key);

        EncryptionKey provider2Key = provider2.getEncryptionKey();
        assertNotNull("provider2 EncryptionKey was not created", provider2Key);

        assertTrue("provider1 key and provider2 key should be equal", Arrays.equals(provider1Key
                .getKey(), provider2Key.getKey()));

        provider1.getManager().clearKey();
        provider2.getManager().clearKey();
    }

    @Test
    public void testCreateTwoProvidersWithDifferentIdentifier() {
        AndroidKeyProvider provider1 = new AndroidKeyProvider(ProviderTestUtil.getContext(),
                ProviderTestUtil.password, ProviderTestUtil.getUniqueIdentifier());
        AndroidKeyProvider provider2 = new AndroidKeyProvider(ProviderTestUtil.getContext(),
                ProviderTestUtil.password, ProviderTestUtil.getUniqueIdentifier());

        EncryptionKey provider1Key = provider1.getEncryptionKey();
        assertNotNull("provider1 EncryptionKey was not created", provider1Key);

        EncryptionKey provider2Key = provider2.getEncryptionKey();
        assertNotNull("provider2 EncryptionKey was not created", provider2Key);

        assertFalse("provider1 key and provider2 key should not be equal", Arrays.equals
                (provider1Key.getKey(), provider2Key.getKey()));

        provider1.getManager().clearKey();
        provider2.getManager().clearKey();
    }

    // Negative tests
    @Test(expected = IllegalArgumentException.class)
    public void testCreateProviderWithNullContext() {
        new AndroidKeyProvider(null, ProviderTestUtil.password, ProviderTestUtil
                .getUniqueIdentifier());
        fail("AndroidKeyProvider constructor should fail if context is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProviderWithNullPassword() {
        new AndroidKeyProvider(ProviderTestUtil.getContext(), null, ProviderTestUtil
                .getUniqueIdentifier());
        fail("AndroidKeyProvider constructor should fail if password is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProviderWithEmptyPassword() {
        new AndroidKeyProvider(ProviderTestUtil.getContext(), "", ProviderTestUtil.getUniqueIdentifier());
        fail("AndroidKeyProvider constructor should fail if password is empty string");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProviderWithNullIdentifier() {
        new AndroidKeyProvider(ProviderTestUtil.getContext(), ProviderTestUtil.password, null);
        fail("AndroidKeyProvider constructor should fail if identifier is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProviderWithEmptyIdentifier() {
        new AndroidKeyProvider(ProviderTestUtil.getContext(), ProviderTestUtil.password, "");
        fail("AndroidKeyProvider constructor should fail if identifier is empty string");
    }

    @Test(expected = DPKException.class)
    public void testLoadWithWrongPassword() {
        String identifier = ProviderTestUtil.getUniqueIdentifier();
        AndroidKeyProvider provider1 = new AndroidKeyProvider(ProviderTestUtil.getContext(),
                ProviderTestUtil.password, identifier);
        AndroidKeyProvider provider2 = new AndroidKeyProvider(ProviderTestUtil.getContext(),
                "wrongpassword", identifier);

        EncryptionKey provider1Key = provider1.getEncryptionKey();
        assertNotNull("provider1 EncryptionKey was not created", provider1Key);

        provider2.getEncryptionKey();
        fail("AndroidKeyProvider getEncryptionKey should fail if password is not correct");

        provider1.getManager().clearKey();
        provider2.getManager().clearKey();
    }
}
