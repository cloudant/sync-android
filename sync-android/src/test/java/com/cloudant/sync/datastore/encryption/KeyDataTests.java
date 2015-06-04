package com.cloudant.sync.datastore.encryption;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class KeyDataTests {

    private KeyData original;

    @Before
    public void setUp() {
        original = ProviderTestUtil.createKeyData();
    }

    @Test
    public void testConstructorGreenPath() {
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

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullDPK() {
        new KeyData(null, original.getSalt(), original.getIv(), original
                .iterations, original.version);
        fail("KeyData constructor should throw IllegalArgumentException if DPK is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyDPK() {
        new KeyData(new byte[0], original.getSalt(), original.getIv(),
                original.iterations, original.version);
        fail("KeyData constructor should throw IllegalArgumentException if DPK is empty");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullSalt() {
        new KeyData(original.getEncryptedDPK(), null, original.getIv(),
                original.iterations, original.version);
        fail("KeyData constructor should throw IllegalArgumentException if Salt is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptySalt() {
        new KeyData(original.getEncryptedDPK(), new byte[0], original.getIv(),
                original.iterations, original.version);
        fail("KeyData constructor should throw IllegalArgumentException if Salt is empty");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullIV() {
        new KeyData(original.getEncryptedDPK(), original.getSalt(), null,
                original.iterations, original.version);
        fail("KeyData constructor should throw IllegalArgumentException if iv is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyIV() {
        new KeyData(original.getEncryptedDPK(), original.getSalt(), null,
                original.iterations, original.version);
        fail("KeyData constructor should throw IllegalArgumentException if iv is empty");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNegativeIterations() {
        new KeyData(original.getEncryptedDPK(), original.getSalt(), original.getIv(),
                -1, original.version);
        fail("KeyData constructor should throw IllegalArgumentException if iterations is " +
                "negative");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullVersion() {
        new KeyData(original.getEncryptedDPK(), original.getSalt(), original.getIv(),
                original.iterations, null);
        fail("KeyData constructor should throw IllegalArgumentException if version is null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyVersion() {
        new KeyData(original.getEncryptedDPK(), original.getSalt(), original.getIv(),
                original.iterations, "");
        fail("KeyData constructor should throw IllegalArgumentException if version is null");
    }
}
