package com.cloudant.sync.datastore.encryption;

import android.test.AndroidTestCase;

import com.cloudant.sync.datastore.encryption.AndroidKeyProvider;
import com.cloudant.sync.datastore.encryption.DPKException;
import com.cloudant.sync.datastore.encryption.EncryptionKey;
import com.cloudant.sync.datastore.encryption.KeyManager;
import com.cloudant.sync.datastore.encryption.KeyStorage;

import java.util.Arrays;

public class AndroidKeyProviderTests extends AndroidTestCase {

    public void testCreateProviderWithIdentifier() {
        AndroidKeyProvider provider = new AndroidKeyProvider(getContext(), ProviderTestUtil
                .password, ProviderTestUtil.getUniqueIdentifier());

        EncryptionKey createdKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not created", createdKey);

        EncryptionKey loadedKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not loaded", loadedKey);

        assertTrue("loadedKey did not match createdKey", Arrays.equals(createdKey.getKey(),
                loadedKey.getKey()));

        provider.getManager().clearKey();
    }

    public void testCreateProviderWithManager() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);
        AndroidKeyProvider provider = new AndroidKeyProvider(ProviderTestUtil.password, manager);

        EncryptionKey createdKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not created", createdKey);

        EncryptionKey loadedKey = provider.getEncryptionKey();
        assertNotNull("EncryptionKey was not loaded", loadedKey);

        assertTrue("loadedKey did not match createdKey", Arrays.equals(createdKey.getKey(),
                loadedKey.getKey()));

        provider.getManager().clearKey();
    }

    public void testCreateTwoProvidersWithSameIdentifier() {
        String identifier = ProviderTestUtil.getUniqueIdentifier();
        AndroidKeyProvider provider1 = new AndroidKeyProvider(getContext(), ProviderTestUtil
                .password, identifier);
        AndroidKeyProvider provider2 = new AndroidKeyProvider(getContext(), ProviderTestUtil
                .password, identifier);

        EncryptionKey provider1Key = provider1.getEncryptionKey();
        assertNotNull("provider1 EncryptionKey was not created", provider1Key);

        EncryptionKey provider2Key = provider2.getEncryptionKey();
        assertNotNull("provider2 EncryptionKey was not created", provider2Key);

        assertTrue("provider1 key and provider2 key should be equal", Arrays.equals(provider1Key
                .getKey(), provider2Key.getKey()));

        provider1.getManager().clearKey();
        provider2.getManager().clearKey();
    }

    public void testCreateTwoProvidersWithDifferentIdentifier() {
        AndroidKeyProvider provider1 = new AndroidKeyProvider(getContext(), ProviderTestUtil
                .password, ProviderTestUtil.getUniqueIdentifier());
        AndroidKeyProvider provider2 = new AndroidKeyProvider(getContext(), ProviderTestUtil
                .password, ProviderTestUtil.getUniqueIdentifier());

        EncryptionKey provider1Key = provider1.getEncryptionKey();
        assertNotNull("provider1 EncryptionKey was not created", provider1Key);

        EncryptionKey provider2Key = provider2.getEncryptionKey();
        assertNotNull("provider2 EncryptionKey was not created", provider2Key);

        assertFalse("provider1 key and provider2 key should not be equal", Arrays.equals
                (provider1Key.getKey(),
                        provider2Key.getKey()));

        provider1.getManager().clearKey();
        provider2.getManager().clearKey();
    }

    // Negative tests
    public void testCreateProviderWithNullContext() {
        try {
            new AndroidKeyProvider(null, ProviderTestUtil.password, ProviderTestUtil
                    .getUniqueIdentifier());
            fail("AndroidKeyProvider constructor should fail if context is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testCreateProviderWithNullManager() {
        try {
            new AndroidKeyProvider("passw0rd", null);
            fail("AndroidKeyProvider constructor should fail if manager is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testCreateProviderWithNullPassword() {
        try {
            new AndroidKeyProvider(getContext(), null, ProviderTestUtil.getUniqueIdentifier());
            fail("AndroidKeyProvider constructor should fail if password is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testCreateProviderWithEmptyPassword() {
        try {
            new AndroidKeyProvider(getContext(), "", ProviderTestUtil.getUniqueIdentifier());
            fail("AndroidKeyProvider constructor should fail if password is empty string");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testCreateProviderWithNullIdentifier() {
        try {
            new AndroidKeyProvider(getContext(), ProviderTestUtil.password, null);
            fail("AndroidKeyProvider constructor should fail if identifier is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testCreateProviderWithEmptyIdentifier() {
        try {
            new AndroidKeyProvider(getContext(), ProviderTestUtil.password, "");
            fail("AndroidKeyProvider constructor should fail if identifier is empty string");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testLoadWithWrongPassword() {
        String identifier = ProviderTestUtil.getUniqueIdentifier();
        AndroidKeyProvider provider1 = new AndroidKeyProvider(getContext(), ProviderTestUtil
                .password,
                identifier);
        AndroidKeyProvider provider2 = new AndroidKeyProvider(getContext(), "wrongpassword",
                identifier);

        EncryptionKey provider1Key = provider1.getEncryptionKey();
        assertNotNull("provider1 EncryptionKey was not created", provider1Key);

        try {
            provider2.getEncryptionKey();
            fail("AndroidKeyProvider getEncryptionKey should fail if password is not correct");
        } catch (DPKException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw DPKException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }

        provider1.getManager().clearKey();
        provider2.getManager().clearKey();
    }

}
