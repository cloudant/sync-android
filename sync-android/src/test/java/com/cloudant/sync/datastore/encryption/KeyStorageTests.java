package com.cloudant.sync.datastore.encryption;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KeyStorageTests {

    // Green Path Tests
    @Test
    public void testKeyDataSaveAndRetrieve() {
        KeyStorage storage = new KeyStorage(ProviderTestUtil.getContext(), ProviderTestUtil
                .getUniqueIdentifier());
        KeyData original = ProviderTestUtil.createKeyData();

        boolean saveSuccess = storage.saveEncryptionKeyData(original);
        assertTrue("saveEncryptionKeyData failed", saveSuccess);

        KeyData savedData = storage.getEncryptionKeyData();

        assertTrue("saved KeyData should be equal to original", keyDataEquals(original, savedData));

        storage.clearEncryptionKeyData();
    }

    @Test
    public void testKeyDataExists() {
        KeyStorage storage = new KeyStorage(ProviderTestUtil.getContext(), ProviderTestUtil
                .getUniqueIdentifier());
        KeyData original = ProviderTestUtil.createKeyData();

        boolean keyDataExists = storage.encryptionKeyDataExists();
        assertFalse("KeyData should not exist", keyDataExists);

        boolean saveSuccess = storage.saveEncryptionKeyData(original);
        assertTrue("saveEncryptionKeyData failed", saveSuccess);

        keyDataExists = storage.encryptionKeyDataExists();
        assertTrue("KeyData should exist", keyDataExists);

        storage.clearEncryptionKeyData();
    }

    @Test
    public void testKeyDataClear() {
        KeyStorage storage = new KeyStorage(ProviderTestUtil.getContext(), ProviderTestUtil
                .getUniqueIdentifier());
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
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullContext() {
        new KeyStorage(null, ProviderTestUtil.getUniqueIdentifier());
        fail("KeyStorage constructor should fail if Context is null");

    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullIdentifier() {
        new KeyStorage(ProviderTestUtil.getContext(), null);
        fail("KeyStorage constructor should fail if identifer is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyStringIdentifier() {
        new KeyStorage(ProviderTestUtil.getContext(), "");
        fail("KeyStorage constructor should fail if identifer is empty string");
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
