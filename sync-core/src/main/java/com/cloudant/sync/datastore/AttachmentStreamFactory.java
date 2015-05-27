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

import com.cloudant.sync.datastore.encryption.EncryptionKey;
import com.cloudant.sync.datastore.encryption.KeyProvider;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;

/**
 * Class to manage returning appropriate streams to access on disk attachments.
 *
 * These may be encrypted and/or gzip encoded. This class handles returning an
 * appropriate input/output stream chain to handle this.
 *
 * The input stream returned will allow reading unencrypted, unzipped data from
 * disk.
 *
 * The output stream returned should have unencrypted, unzipped data written to
 * its write method.
 *
 */
class AttachmentStreamFactory {

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
     * @throws IOException
     */
    public InputStream getInputStream(File file, Attachment.Encoding encoding) throws IOException {

        InputStream is = new FileInputStream(file);

        if (key != null) {
            // TODO wrap encrypted stream

            // For InputStream:
            // 1. Read header, check valid, throw if not
            // 3. Init AES Cipher object (wrapped by our header-writing stream)
            // 4. Forward on requests to read() to AES CipherOutputStream
            // 5. Close appropriately

            throw new UnsupportedOperationException("Encryption not yet supported");
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
     * Data should be written to the stream un-encoded, un-encrypted.
     *
     * @param file File to write to.
     * @param encoding Encoding to use.
     * @return Stream for writing.
     * @throws IOException
     */
    public OutputStream getOutputStream(File file, Attachment.Encoding encoding) throws
            IOException {

        OutputStream os = FileUtils.openOutputStream(file);

        if (key != null) {
            // TODO wrap encrypted stream

            // For OutputStream:
            // 1. Generate an IV
            // 2. Write header to os
            // 3. Init AES Cipher object (wrapped by our header-writing stream)
            // 4. Forward on requests to write() to AES CipherOutputStream
            // 5. Close appropriately

            throw new UnsupportedOperationException("Encryption not yet supported");
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
