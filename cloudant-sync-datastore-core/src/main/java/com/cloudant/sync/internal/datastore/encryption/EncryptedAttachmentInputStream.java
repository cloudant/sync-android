/**
 * Copyright © 2015 IBM Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.internal.datastore.encryption;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.NullCipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * <p>An EncryptedAttachmentInputStream handles reading AES encrypted data from disk,
 * first reading the attachment file's header to determine the IV and checking the
 * on-disk version is readable.
 * A CipherInputStream is used to decrypt the data read via read() methods.</p>
 *
 * <p>This class adheres strictly to the semantics, especially the failure semantics, of its
 * ancestor classes java.io.FilterInputStream and java.io.InputStream. This class has exactly
 * those methods specified in its ancestor classes, and overrides them all. Moreover, this
 * class catches all exceptions that are not thrown by its ancestor classes. In particular,
 * the skip method skips, and the available method counts only data that have been processed
 * by the encapsulated Cipher.</p>
 *
 * <p>It is crucial for a programmer using this class not to use methods that are not defined
 * or overriden in this class (such as a new method or constructor that is later added to
 * one of the super classes), because the design and implementation of those methods are
 * unlikely to have considered security impact with regard to CipherInputStream.</p>
 *
 * <p>The data format read by this class is written by EncryptedAttachmentOutputStream, and
 * is:</p>
 *
 * <pre>
 * Header:
 * 1-byte : version number, 1
 * 16-byte: initialisation vector
 *
 * Body:
 * AES CBC Encrypted file content.
 * </pre>
 *
 * @api_private
 */
public class EncryptedAttachmentInputStream extends FilterInputStream {

    private final CipherInputStream cipherInputStream;

    /**
     * <p>Creates an input stream without specifying a key.</p>
     *
     * <p>This is essentially useless.</p>
     *
     * @param in the input stream object.
     */
    protected EncryptedAttachmentInputStream(InputStream in) {
        super(in);
        cipherInputStream = new CipherInputStream(in, new NullCipher());
    }

    /**
     * <p>Creates an input stream with a key.</p>
     *
     * <p>An IV is not required as the on-disk format contains this.</p>
     *
     * <p>This constructor reads bytes from the in parameter in order to read the header.</p>
     *
     * <p>Note: if the specified input stream is null, a NullPointerException is thrown as
     * the constructor tries to read from the passed input stream.</p>
     *
     * @param in the input stream object.
     * @param key the encryption key to use. Length must be supported by underlying
     *            JCE implementation.
     *
     * @throws InvalidKeyException if key is wrong size
     * @throws IOException on I/O exceptions
     */
    public EncryptedAttachmentInputStream(InputStream in, byte[] key)
            throws InvalidKeyException, IOException {

        super(in);

        Cipher c;

        // Don't change under our feet
        byte[] keyCopy = Arrays.copyOf(key, key.length);

        try {

            // Be sure Cipher is valid with passed parameters before reading anything
            c = Cipher.getInstance(EncryptionConstants.CIPHER);
            c.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(keyCopy, EncryptionConstants.KEY_ALGORITHM),
                    new IvParameterSpec(new byte[16]));  // Empty IV to test key length, don't reuse!

            int read;

            // Read version, check correct - 1-byte
            byte[] version = new byte[1];
            read = in.read(version);
            if (read != 1) {
                throw new IOException("Could not read version from file header.");
            }
            if (version[0] <= 0 || version[0] > EncryptionConstants.ATTACHMENT_DISK_VERSION) {
                throw new IOException("Unsupported on-disk version for attachment decryption.");
            }

            // Read IV - 16-bytes
            byte[] ivBuffer = new byte[16];
            read = in.read(ivBuffer);
            if (read != 16) {
                throw new IOException("Could not read initialisation vector from file header.");
            }

            // Decrypt cipher text - rest of file
            c = Cipher.getInstance(EncryptionConstants.CIPHER);
            c.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(keyCopy, EncryptionConstants.KEY_ALGORITHM),
                    new IvParameterSpec(ivBuffer));
            cipherInputStream = new CipherInputStream(in, c);

        } catch (NoSuchPaddingException ex) {
            // Should not happen, padding should be supported by every JCE, so wrap in RuntimeEx
            throw new RuntimeException("Couldn't initialise crypto engine", ex);
        } catch (NoSuchAlgorithmException ex) {
            // Should not happen, AES should be supported by every JCE, so wrap in RuntimeException
            throw new RuntimeException("Couldn't initialise crypto engine", ex);
        } catch (InvalidAlgorithmParameterException ex) {
            // Should not happen, 16 byte IV for AES is correct, so wrap in RuntimeEx
            throw new RuntimeException("Couldn't initialise crypto engine", ex);
        }
    }

    @Override
    /**
     * <p>Reads the next byte of data from this input stream. The value byte is returned as an
     * int in the range 0 to 255. If no byte is available because the end of the stream has been
     * reached, the value -1 is returned. This method blocks until input data is available, the
     * end of the stream is detected, or an exception is thrown.<p>
     *
     * <p>This method proxies to the CipherInputStream.</p>
     * @throws IOException
     */
    public int read() throws IOException {
        return cipherInputStream.read();
    }

    @Override
    /**
     * <p>Reads up to b.length bytes of data from this input stream into an array of bytes.</p>
     *
     * <p>This method proxies to the CipherInputStream.</p>
     *
     * @param b the buffer into which the data is read
     *
     * @return the total number of bytes read into the buffer, or -1 is there is no more data
     *          because the end of the stream has been reached
     * @throws IOException
     */
    public int read(byte[] b)
            throws IOException {
        return cipherInputStream.read(b);
    }

    @Override
    /**
     * <p>Reads up to len bytes of data from this input stream into an array of bytes. This
     * method blocks until some input is available. If the first argument is null, up to len
     * bytes are read and discarded.</p>
     *
     * @param b the buffer into which the data is read.
     * @param off the start offset in the destination array buf
     * @param len the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data
     *          because the end of the stream has been reached.
     * @throws IOException
     */
    public int read(byte[] b,
                    int off,
                    int len)
            throws IOException {
        return cipherInputStream.read(b, off, len);
    }

    @Override
    /**
     * <p>Skips n bytes of input from the bytes that can be read from this input stream
     * without blocking.</p>
     *
     * <p>Fewer bytes than requested might be skipped. The actual number of bytes skipped is
     * equal to n or the result of a call to available, whichever is smaller. If n is less
     * than zero, no bytes are skipped.</p>
     *
     * <p>The actual number of bytes skipped is returned.</p>
     *
     * @param n the number of bytes to be skipped.
     * @return the actual number of bytes skipped.
     * @throws IOException
     */
    public long skip(long n)
            throws IOException {
        return cipherInputStream.skip(n);
    }

    @Override
    /**
     * <p>Returns the number of bytes that can be read from this input stream without
     * blocking. The available method of InputStream returns 0. This method should be
     * overridden by subclasses.</p>
     *
     * @return the number of bytes that can be read from this input stream without blocking.
     * @throws IOException
     */
    public int available()
            throws IOException {
        return cipherInputStream.available();
    }

    @Override
    /**
     * <p>Closes this input stream and releases any system resources associated with the stream.</p>
     *
     * <p>This method calls close on its proxied CipherInputStream.</p>
     * @throws IOException
     */
    public void close()
            throws IOException {
        cipherInputStream.close();
    }

    @Override
    /**
     * <p>Tests if this input stream supports the mark and reset methods, which it does not.</p>
     *
     * <p>This method proxies to the underlying CipherInputStream, which don't support mark
     * and reset.</p>
     *
     * @return false, since the underlying method call always returns false.
     */
    public boolean markSupported() {
        return cipherInputStream.markSupported();
    }

}
