/*
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

import java.io.IOException;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.NullCipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * <p>An EncryptedAttachmentOutputStream handles writing AES encrypted data to disk,
 * combining the encrypted data with a header specifying the IV used during encryption.
 * A CipherOutputStream is used to encrypt the data sent via write() methods.</p>
 *
 * <p>This class adheres strictly to the semantics, especially the failure semantics,
 * of its ancestor classes java.io.OutputStream and java.io.FilterOutputStream. This class
 * has exactly those methods specified in its ancestor classes, and overrides them all.
 * Moreover, this class catches all exceptions that are not thrown by its ancestor classes.</p>
 *
 * <p>It is crucial for a programmer using this class not to use methods that are not
 * defined or overriden in this class (such as a new method or constructor that is later
 * added to one of the super classes), because the design and implementation of those
 * methods are unlikely to have considered security impact with regard to the underlying
 * CipherOutputStream.</p>
 *
 * <p>The data format written by this class is read by EncryptedAttachmentInputStream, and
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
public class EncryptedAttachmentOutputStream extends java.io.FilterOutputStream {

    private final CipherOutputStream cipherOutputStream;

    /**
     * <p>Creates an output stream without specifying a key or iv.</p>
     *
     * <p>This is essentially useless.</p>
     *
     * @param out the output stream object.
     */
    protected EncryptedAttachmentOutputStream(OutputStream out) {
        super(out);
        cipherOutputStream = new CipherOutputStream(out, new NullCipher());
    }

    /**
     * <p>Creates an output stream with a key and IV.</p>
     *
     * <p>This constructor writes bytes to the out parameter in order to write the header.</p>
     *
     * <p>Note: if the specified output stream is null, a NullPointerException is thrown as
     * the constructor tries to write to the passed output stream.</p>
     *
     * @param out the output stream object.
     * @param key the encryption key to use. Length must be supported by underlying
     *            JCE implementation.
     * @param iv the initialisation vector to use.
     *
     * @throws InvalidAlgorithmParameterException if IV is wrong size
     * @throws InvalidKeyException if key is wrong size
     * @throws IOException on I/O exceptions
     */
    public EncryptedAttachmentOutputStream(OutputStream out, byte[] key, byte[] iv)
            throws InvalidAlgorithmParameterException, InvalidKeyException, IOException {
        super(out);

        // Don't change under our feet
        byte[] keyCopy = Arrays.copyOf(key, key.length);
        byte[] ivCopy = Arrays.copyOf(iv, iv.length);

        try {

            // Be sure Cipher is valid with passed parameters before writing anything
            Cipher c = Cipher.getInstance(EncryptionConstants.CIPHER);
            c.init(Cipher.ENCRYPT_MODE,
                    new SecretKeySpec(keyCopy, EncryptionConstants.KEY_ALGORITHM),
                    new IvParameterSpec(ivCopy));

            // Write header
            out.write(new byte[]{EncryptionConstants.ATTACHMENT_DISK_VERSION});
            out.write(ivCopy);

            // Ready to write the encrypted body
            cipherOutputStream = new CipherOutputStream(out, c);

        } catch (NoSuchPaddingException ex) {
            // Should not happen, padding should be supported by every JCE, so wrap in RuntimeEx
            throw new RuntimeException("Couldn't initialise crypto engine", ex);
        } catch (NoSuchAlgorithmException ex) {
            // Should not happen, AES should be supported by every JCE, so wrap in RuntimeException
            throw new RuntimeException("Couldn't initialise crypto engine", ex);
        }
    }

    @Override
    /**
     * <p>Closes this output stream and releases any system resources associated with this
     * stream.</p>
     *
     * <p>This just proxies to the underlying CipherOutputStream, which:</p>
     *
     * <ul>
     * <li>Invokes the doFinal method of the encapsulated cipher object, which
     * causes any bytes buffered by the encapsulated cipher to be processed. The result is
     * written out by calling the flush method of this output stream.</li>
     * <li>Resets the encapsulated cipher object to its initial state and calls
     * the close method of the underlying output stream.</li>
     * </ul>
     */
    public void close() throws IOException {
        cipherOutputStream.close();
    }

    @Override
    /**
     * <p>Flushes this output stream by forcing any buffered output bytes that have already
     * been processed by the encapsulated cipher object to be written out.</p>
     *
     * <p>Any bytes buffered by the encapsulated cipher and waiting to be processed by it
     * will not be written out. For example, if the encapsulated cipher is a block cipher,
     * and the total number of bytes written using one of the write methods is less than
     * the cipher's block size, no bytes will be written out.</p>
     *
     * <p>This just proxies to the underlying CipherOutputStream.</p>
     */
    public void flush() throws IOException {
        cipherOutputStream.flush();
    }

    @Override
    /**
     * <p>Writes b.length bytes from the specified byte array to this output stream.</p>
     *
     * <p>This just proxies to the underlying CipherOutputStream.</p>
     *
     * @param b the data
     */
    public void write(byte[] b) throws IOException {
        cipherOutputStream.write(b);
    }

    @Override
    /**
     * <p>Writes len bytes from the specified byte array starting at offset off to this
     * output stream.</p>
     *
     * <p>This just proxies to the underlying CipherOutputStream.</p>
     *
     * @param b the data
     * @param off the start offset in the data
     * @param len the number of bytes to write
     */
    public void write(byte[] b, int off, int len) throws IOException {
        cipherOutputStream.write(b, off, len);
    }

    @Override
    /**
     * <p>Writes the specified byte to this output stream.</p>
     *
     * <p>This just proxies to the underlying CipherOutputStream.</p>
     *
     * @param b the data
     */
    public void write(int b) throws IOException {
        cipherOutputStream.write(b);
    }
}
