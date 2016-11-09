package com.cloudant.sync.datastore.encryption;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

import com.cloudant.sync.documentstore.encryption.EncryptionKey;

public class KeyManagerTests {
    private KeyManager manager;
    private KeyStorage storage;

    @Before
    public void beforeMethod() {
        assumeNotNull(ProviderTestUtil.getContext());
        storage = new KeyStorage(ProviderTestUtil.getContext(), ProviderTestUtil.getUniqueIdentifier());
        manager = new KeyManager(storage);
    }

    @After
    public void afterMethod() throws Exception {
        if(manager != null)
            manager.clearKey();
    }

    @Test
    public void testGenerateDPK() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());
    }

    @Test
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

    @Test
    public void testLoadBeforeGenerateDPK() {
        EncryptionKey storedDPK = manager.loadKeyUsingPassword(ProviderTestUtil.password);
        assertNull("DPK should not be null", storedDPK);
    }

    @Test
    public void testDPKExists() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());

        boolean keyExists = manager.keyExists();
        assertTrue("DPK should exist but does not exist", keyExists);
    }

    @Test
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

    @Test
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
    @Test(expected = IllegalArgumentException.class)
    public void testGenerateDPKWithNullPassword() {
        manager.generateAndSaveKeyProtectedByPassword(null);
        fail("KeyManager generateAndSaveKeyProtectedByPassword should fail if password is " +
                "null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGenerateDPKWithEmptyPassword() {
        manager.generateAndSaveKeyProtectedByPassword("");
        fail("KeyManager generateAndSaveKeyProtectedByPassword should fail if password is " +
                "empty string");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadDPKWithNullPassword() {
        manager.loadKeyUsingPassword(null);
        fail("KeyManager loadKeyUsingPassword should fail if password is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadDPKWithEmptyPassword() {
        manager.loadKeyUsingPassword("");
        fail("KeyManager loadKeyUsingPassword should fail if password is empty string");
    }

    @Test(expected = DPKException.class)
    public void testLoadWithWrongPassword() {
        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(ProviderTestUtil
                .password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());

        manager.loadKeyUsingPassword("wrongpass");
        fail("KeyManager loadKeyUsingPassword should fail if password is not correct");
    }
}
