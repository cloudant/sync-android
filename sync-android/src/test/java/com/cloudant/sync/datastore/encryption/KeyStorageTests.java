package com.cloudant.sync.datastore.encryption;

import android.test.AndroidTestCase;

import com.cloudant.sync.datastore.encryption.KeyData;
import com.cloudant.sync.datastore.encryption.KeyStorage;

import java.util.Arrays;

public class KeyStorageTests extends AndroidTestCase {

    // Green Path Tests
    public void testKeyDataSaveAndRetrieve() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyData original = ProviderTestUtil.createKeyData();

        boolean saveSuccess = storage.saveEncryptionKeyData(original);
        assertTrue("saveEncryptionKeyData failed", saveSuccess);

        KeyData savedData = storage.getEncryptionKeyData();

        assertTrue("saved KeyData should be equal to original", keyDataEquals(original, savedData));

        storage.clearEncryptionKeyData();
    }

    public void testKeyDataExists() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyData original = ProviderTestUtil.createKeyData();

        boolean keyDataExists = storage.encryptionKeyDataExists();
        assertFalse("KeyData should not exist", keyDataExists);

        boolean saveSuccess = storage.saveEncryptionKeyData(original);
        assertTrue("saveEncryptionKeyData failed", saveSuccess);

        keyDataExists = storage.encryptionKeyDataExists();
        assertTrue("KeyData should exist", keyDataExists);

        storage.clearEncryptionKeyData();
    }

    public void testKeyDataClear() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyData original = ProviderTestUtil.createKeyData();

        boolean saveSuccess = storage.saveEncryptionKeyData(original);
        assertTrue("saveEncryptionKeyData failed", saveSuccess);

        boolean keyDataExists = storage.encryptionKeyDataExists();
        assertTrue("KeyData should exist", keyDataExists);

        boolean dataCleared = storage.clearEncryptionKeyData();
        assertTrue("KeyData should not exist", dataCleared);

        KeyData savedKeyData = storage.getEncryptionKeyData();
        assertNull("KeyData should not exist", savedKeyData);

        keyDataExists = storage.encryptionKeyDataExists();
        assertFalse("KeyData should exist", keyDataExists);

        storage.clearEncryptionKeyData();
    }

    // Negative Tests
    public void testConstructorNullContext() {
        try {
            new KeyStorage(null, ProviderTestUtil.getUniqueIdentifier());
            fail("KeyStorage constructor should fail if Context is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }

    }

    public void testConstructorNullIdentifier() {
        try {
            new KeyStorage(getContext(), null);
            fail("KeyStorage constructor should fail if identifer is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorEmptyStringIdentifier() {
        try {
            new KeyStorage(getContext(), "");
            fail("KeyStorage constructor should fail if identifer is empty string");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    // Helper methods
    private boolean keyDataEquals(KeyData data1, KeyData data2) {
        if (data1 == null || data2 == null) {
            return false;
        }
        return Arrays.equals(data1.getEncryptedDPK(), data2.getEncryptedDPK()) && Arrays.equals
                (data1.getSalt(), data2.getSalt()) && Arrays.equals(data1.getIv(),
                data2.getIv()) && data1.iterations == data2.iterations && data1
                .version.equals(data2.version);
    }

}
