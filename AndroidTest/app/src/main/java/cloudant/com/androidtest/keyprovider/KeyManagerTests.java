package cloudant.com.androidtest.keyprovider;

import android.test.AndroidTestCase;

import com.cloudant.sync.datastore.encryption.DPKException;
import com.cloudant.sync.datastore.encryption.EncryptionKey;
import com.cloudant.sync.datastore.encryption.KeyManager;
import com.cloudant.sync.datastore.encryption.KeyStorage;

import java.util.Arrays;

public class KeyManagerTests extends AndroidTestCase {
    public void testGenerateDPK() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

        String password = "passw0rd";

        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());
        assertTrue("byte[] of DPK should not be empty", dpk.getKey().length > 0);
    }

    public void testLoadDPK() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

        String password = "passw0rd";

        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());
        assertTrue("byte[] of DPK should not be empty", dpk.getKey().length > 0);

        EncryptionKey storedDPK = manager.loadKeyUsingPassword(password);
        assertNotNull("DPK should not be null", storedDPK);
        assertNotNull("byte[] of DPK should not be null", storedDPK.getKey());
        assertTrue("byte[] of DPK should not be empty", storedDPK.getKey().length > 0);

        assertTrue(Arrays.equals(dpk.getKey(), storedDPK.getKey()));
    }

    public void testDPKExists() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

        String password = "passw0rd";

        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());
        assertTrue("byte[] of DPK should not be empty", dpk.getKey().length > 0);

        boolean keyExists = manager.keyExists();
        assertTrue("DPK should exist but does not exist", keyExists);
    }

    public void testDPKClear() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

        String password = "passw0rd";

        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());
        assertTrue("byte[] of DPK should not be empty", dpk.getKey().length > 0);

        boolean keyExists = manager.keyExists();
        assertTrue("DPK should exist but does not exist", keyExists);

        EncryptionKey storedDPK = manager.loadKeyUsingPassword(password);
        assertNotNull("DPK should not be null", storedDPK);
        assertNotNull("byte[] of DPK should not be null", storedDPK.getKey());
        assertTrue("byte[] of DPK should not be empty", storedDPK.getKey().length > 0);

        boolean clearSuccess = manager.clearKey();
        assertTrue("clear key failed", clearSuccess);

        storedDPK = manager.loadKeyUsingPassword(password);
        assertNull("DPK should be null", storedDPK);

        keyExists = manager.keyExists();
        assertFalse("DPK should not exist but does not exist", keyExists);
    }

    public void testDPKRecreateAfterClear() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

        String password = "passw0rd";

        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());
        assertTrue("byte[] of DPK should not be empty", dpk.getKey().length > 0);

        boolean keyExists = manager.keyExists();
        assertTrue("DPK should exist but does not exist", keyExists);

        boolean clearSuccess = manager.clearKey();
        assertTrue("clear key failed", clearSuccess);

        keyExists = manager.keyExists();
        assertFalse("DPK should not exist but does not exist", keyExists);

        EncryptionKey newDPK = manager.generateAndSaveKeyProtectedByPassword(password);
        assertNotNull("DPK should not be null", newDPK);
        assertNotNull("byte[] of DPK should not be null", newDPK.getKey());
        assertTrue("byte[] of DPK should not be empty", newDPK.getKey().length > 0);

        assertFalse("newKey should not be the same as the original deleted key", Arrays.equals
                (dpk.getKey(), newDPK.getKey()));
    }

    // Negative tests
    public void testGenerateDPKWithNullPassword() {
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

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
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

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
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

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
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

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
        KeyStorage storage = new KeyStorage(getContext(), ProviderTestUtil.getUniqueIdentifier());
        KeyManager manager = new KeyManager(storage);

        String password = "passw0rd";

        EncryptionKey dpk = manager.generateAndSaveKeyProtectedByPassword(password);
        assertNotNull("DPK should not be null", dpk);
        assertNotNull("byte[] of DPK should not be null", dpk.getKey());
        assertTrue("byte[] of DPK should not be empty", dpk.getKey().length > 0);

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
