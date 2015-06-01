package com.cloudant.sync.datastore.encryption;

import android.test.AndroidTestCase;

import com.cloudant.sync.datastore.encryption.DPKException;
import com.cloudant.sync.datastore.encryption.EncryptionKey;
import com.cloudant.sync.datastore.encryption.KeyManager;
import com.cloudant.sync.datastore.encryption.KeyStorage;

import java.util.Arrays;

public class KeyManagerTests extends AndroidTestCase {
    private KeyManager manager;
    private KeyStorage storage;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        manager = new KeyManager(storage);
    }

    @Override
    protected void tearDown() throws Exception {
        manager.clearKey();
        super.tearDown();
    }

    public void testGenerateDPK() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());
    }

    public void testLoadDPK() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());

        EncryptionKey storedDPK = manager.loadKeyUsingPassword(ProviderTestUtil.password);
        assertNotNull("DPK should not be null", storedDPK);
        assertNotNull("byte[] of DPK should not be null", storedDPK.getKey());

        assertTrue(Arrays.equals(dpk.getKey(), storedDPK.getKey()));
    }

    public void testLoadBeforeGenerateDPK() {
        EncryptionKey storedDPK = manager.loadKeyUsingPassword(ProviderTestUtil.password);
        assertNull("DPK should not be null", storedDPK);
    }

    public void testDPKExists() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());

        boolean keyExists = manager.keyExists();
        assertTrue("DPK should exist but does not exist", keyExists);
    }

    public void testDPKClear() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());

        boolean keyExists = manager.keyExists();
        assertTrue("DPK should exist but does not exist", keyExists);

        EncryptionKey storedDPK = manager.loadKeyUsingPassword(ProviderTestUtil.password);
        assertNotNull("DPK should not be null", storedDPK);
        assertNotNull("byte[] of DPK should not be null", storedDPK.getKey());

        boolean clearSuccess = manager.clearKey();
        assertTrue("clear key failed", clearSuccess);

        storedDPK = manager.loadKeyUsingPassword(ProviderTestUtil.password);
        assertNull("DPK should be null", storedDPK);

        keyExists = manager.keyExists();
        assertFalse("DPK should not exist but does not exist", keyExists);
    }

    public void testDPKRecreateAfterClear() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());

        boolean keyExists = manager.keyExists();
        assertTrue("DPK should exist but does not exist", keyExists);

        boolean clearSuccess = manager.clearKey();
        assertTrue("clear key failed", clearSuccess);

        keyExists = manager.keyExists();
        assertFalse("DPK should not exist but does not exist", keyExists);

        EncryptionKey newDPK = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", newDPK);
        assertNotNull("byte[] of DPK should not be null", newDPK.getKey());

        assertFalse("newKey should not be the same as the original deleted key", Arrays.equals
                (dpk.getKey(), newDPK.getKey()));
    }

    // Negative tests
    public void testGenerateDPKWithNullPassword() {
        try {
            manager.generateAndSaveKeyProtectedByPassword(null);
            fail("KeyManager generateAndSaveKeyProtectedByPassword should fail if password is " +
                    "null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testGenerateDPKWithEmptyPassword() {
        try {
            manager.generateAndSaveKeyProtectedByPassword("");
            fail("KeyManager generateAndSaveKeyProtectedByPassword should fail if password is " +
                    "empty string");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testLoadDPKWithNullPassword() {
        try {
            manager.loadKeyUsingPassword(null);
            fail("KeyManager loadKeyUsingPassword should fail if password is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testLoadDPKWithEmptyPassword() {
        try {
            manager.loadKeyUsingPassword("");
            fail("KeyManager loadKeyUsingPassword should fail if password is empty string");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testLoadWithWrongPassword() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());

        try {
            manager.loadKeyUsingPassword("wrongpass");
            fail("KeyManager loadKeyUsingPassword should fail if password is not correct");
        } catch (DPKException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw DPKException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }
}
