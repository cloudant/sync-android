package com.cloudant.sync.datastore.encryption;

import android.test.AndroidTestCase;

import com.cloudant.sync.datastore.encryption.KeyData;

import java.util.Arrays;

public class KeyDataTests extends AndroidTestCase {

    public void testConstructorGreenPath() {
        KeyData original = ProviderTestUtil.createKeyData();
        KeyData newData = new KeyData(original.getEncryptedDPK(), original.getSalt(), original
                .getIv(), original.iterations, original.version);

        assertTrue("newData encryptedDPK didn't match original", Arrays.equals(original
                .getEncryptedDPK(), newData.getEncryptedDPK()));
        assertTrue("newData iv didn't match original", Arrays.equals(original
                .getIv(), newData.getIv()));
        assertTrue("newData salt didn't match original", Arrays.equals(original
                .getSalt(), newData.getSalt()));
        assertTrue("newData iterations didn't match original", original.iterations == newData
                .iterations);
        assertTrue("newData versions didn't match original", original.version.equals(newData
                .version));
    }

    public void testConstructorNullDPK() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(null, original.getSalt(), original.getIv(), original
                    .iterations, original.version);
            fail("KeyData constructor should throw IllegalArgumentException if DPK is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorEmptyDPK() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(new byte[0], original.getSalt(), original.getIv(),
                    original.iterations, original.version);
            fail("KeyData constructor should throw IllegalArgumentException if DPK is empty");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorNullSalt() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(original.getEncryptedDPK(), null, original.getIv(),
                    original.iterations, original.version);
            fail("KeyData constructor should throw IllegalArgumentException if Salt is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorEmptySalt() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(original.getEncryptedDPK(), new byte[0], original.getIv(),
                    original.iterations, original.version);
            fail("KeyData constructor should throw IllegalArgumentException if Salt is empty");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorNullIV() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(original.getEncryptedDPK(), original.getSalt(), null,
                    original.iterations, original.version);
            fail("KeyData constructor should throw IllegalArgumentException if iv is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorEmptyIV() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(original.getEncryptedDPK(), original.getSalt(), null,
                    original.iterations, original.version);
            fail("KeyData constructor should throw IllegalArgumentException if iv is empty");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorNegativeIterations() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(original.getEncryptedDPK(), original.getSalt(), original.getIv(),
                    -1, original.version);
            fail("KeyData constructor should throw IllegalArgumentException if iterations is " +
                    "negative");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorNullVersion() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(original.getEncryptedDPK(), original.getSalt(), original.getIv(),
                    original.iterations, null);
            fail("KeyData constructor should throw IllegalArgumentException if version is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }

    public void testConstructorEmptyVersion() {
        KeyData original = ProviderTestUtil.createKeyData();
        try {
            new KeyData(original.getEncryptedDPK(), original.getSalt(), original.getIv(),
                    original.iterations, "");
            fail("KeyData constructor should throw IllegalArgumentException if version is null");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        } catch (Throwable t) {
            fail("Failed to throw IllegalArgumentException.  Found: " + t.getClass()
                    .getSimpleName() + ": " + t.getLocalizedMessage());
        }
    }
}
