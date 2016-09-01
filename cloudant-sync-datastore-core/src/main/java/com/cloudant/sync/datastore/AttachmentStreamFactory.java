/**
 * Copyright (c) 2015 IBM Cloudant, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore;

import com.cloudant.sync.datastore.encryption.EncryptedAttachmentInputStream;
import com.cloudant.sync.datastore.encryption.EncryptedAttachmentOutputStream;
import com.cloudant.sync.datastore.encryption.KeyProvider;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Class to manage returning appropriate streams to access on disk attachments.
 *
 * These may be encrypted and/or gzip encoded. This class handles returning an
 * appropriate input/output stream chain to handle this.
 *
 * The input stream returned will contain the unencrypted, unzipped representation
 * of the data from disk.
 *
 * The output stream returned should have unencrypted, unzipped data written to
 * its write method.
 *
 * @api_private
 */
public class AttachmentStreamFactory {

    /**
     * Byte array if there is a valid AES key, null if attachments
     * should not be encrypted.
     */
    private final byte[] key;

    /**
     * Creates a factory Attachment objects can use to read/write attachments.
     *
     * @param keyProvider Datastore's key provider object. This object can cope with key providers
     *                    that return null keys.
     */
    public AttachmentStreamFactory(KeyProvider keyProvider) {
        if (keyProvider.getEncryptionKey() != null) {
            // Key guaranteed to be 256-bits by EncryptionKey class
            this.key = keyProvider.getEncryptionKey().getKey();
        } else {
            this.key = null;
        }
    }

    /**
     * Return a stream to be used to read from the file on disk.
     *
     * Stream's bytes will be unzipped, unencrypted attachment content.
     *
     * @param file File object to read from.
     * @param encoding Encoding of attachment.
     * @return Stream for reading attachment data.
     * @throws IOException if there's a problem reading from disk, including issues with
     *      encryption (bad key length and other key issues).
     */
    public InputStream getInputStream(File file, Attachment.Encoding encoding) throws IOException {

        // First, open a stream to the raw bytes on disk.
        // Then, if we have a key assume the file is encrypted, so add a stream
        // to the chain which decrypts the data as we read from disk.
        // Finally, decode (unzip) the data if the attachment is encoded before
        // returning the data to the user.
        //
        //  Read from disk [-> Decryption Stream] [-> Decoding Stream] -> user reads

        InputStream is = new FileInputStream(file);

        if (key != null) {
            try {
                is = new EncryptedAttachmentInputStream(is, key);
            } catch (InvalidKeyException ex) {
                // Replace with an IOException as we validate the key when opening
                // the databases and the key should be the same -- it's therefore
                // not worth forcing the developer to catch something they can't
                // fix during file read; generic IOException works better.
                throw new IOException("Bad key used to open file; check encryption key.", ex);
            }
        }

        switch (encoding) {
            case Plain:
                break;  // nothing to do
            case Gzip:
                is = new GZIPInputStream(is);
        }

        return is;
    }

    /**
     * Get stream for writing attachment data to disk.
     *
     * Opens the output stream using {@see FileUtils#openOutputStream(File)}.
     *
     * Data should be written to the stream unencoded, unencrypted.
     *
     * @param file File to write to.
     * @param encoding Encoding to use.
     * @return Stream for writing.
     * @throws IOException if there's a problem writing to disk, including issues with
     *      encryption (bad key length and other key issues).
     */
    public OutputStream getOutputStream(File file, Attachment.Encoding encoding) throws
            IOException {

        // First, open a stream to the raw bytes on disk.
        // Then, if we have a key assume the file should be encrypted before writing,
        // so wrap the file stream in a stream which encrypts during writing.
        // If the attachment needs encoding, we need to encode the data before it
        // is encrypted, so wrap a stream which will encode (gzip) before passing
        // to encryption stream, or directly to file stream if not encrypting.
        //
        //  User writes [-> Encoding Stream] [-> Encryption Stream] -> write to disk

        OutputStream os = FileUtils.openOutputStream(file);

        if (key != null) {

            try {

                // Create IV
                byte[] iv = new byte[16];
                new SecureRandom().nextBytes(iv);

                os = new EncryptedAttachmentOutputStream(os, key, iv);

            } catch (InvalidKeyException ex) {
                // Replace with an IOException as we validate the key when opening
                // the databases and the key should be the same -- it's therefore
                // not worth forcing the developer to catch something they can't
                // fix during file read; generic IOException works better.
                throw new IOException("Bad key used to write file; check encryption key.", ex);
            } catch (InvalidAlgorithmParameterException ex) {
                // We are creating what should be a valid IV for AES, 16-bytes.
                // Therefore this shouldn't happen. Again, the developer cannot
                // fix it, so wrap in an IOException as we can't write the file.
                throw new IOException("Bad key used to write file; check encryption key.", ex);
            }

        }

        switch (encoding) {
            case Plain:
                break;  // nothing to do
            case Gzip:
                os = new GZIPOutputStream(os);
        }

        return os;
    }

}
