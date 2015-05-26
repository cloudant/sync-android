/**
 * Copyright (c) 2015 IBM Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.sqlite.android.encryption;

import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.KeyUtils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Created by estebanmlaver.
 */
public class AttachmentStreamFactory {

    private static final int ANDROID_BUFFER_8K = 8192;

    private static Attachment.Encoding encoding;
    private static KeyProvider provider;

    private static AttachmentStreamFactory singleton;

    private AttachmentStreamFactory(){ }

    public static synchronized AttachmentStreamFactory getInstance() {
        if (singleton == null) {
            singleton = new AttachmentStreamFactory();
        }
        return singleton;
    }


    /*public static InputStream getStream(Attachment.Encoding encoding, File file, KeyProvider provider) throws IOException {
        InputStream inputStream = null;
        //Get key from provider
        String sqlcipherKey = null;
        boolean isEncryption = false;
        if(provider != null) {
            sqlcipherKey = KeyUtils.sqlCipherKeyForKeyProvider(provider);
            //Check that key exists
            if(!sqlcipherKey.isEmpty()) {
                isEncryption = true;
            }
        }

        unwrapEncoding(encoding, sqlcipherKey, file, isEncryption);

        return inputStream;
    }*/

    public static InputStream openStream(InputStream attachmentStream, KeyProvider provider) throws IOException {
        InputStream inputStream = null;
        //Get key from provider
        String sqlcipherKey = null;
        boolean isEncryption = false;
        if(provider != null) {
            sqlcipherKey = KeyUtils.sqlCipherKeyForKeyProvider(provider);
            //Check that key exists
            if(!sqlcipherKey.isEmpty()) {
                isEncryption = true;
            }
        }

        unwrapEncoding(attachmentStream, encoding, sqlcipherKey, isEncryption);

        return inputStream;
    }

    /*private static InputStream unwrapEncoding(Attachment.Encoding encoding, String sqlcipherKey, boolean isEncryption)
            throws IOException {
        if(encoding == Attachment.Encoding.Gzip) {
            if(isEncryption) {
                return unwrapEncryption(sqlcipherKey, true);
            } else {
                //Send input stream with GZIP
                return new GZIPInputStream(new FileInputStream(file));
            }
        } else {
            if(isEncryption) {
                return unwrapEncryption(sqlcipherKey, file, false);
            } else {
                //Return file input stream
                return new FileInputStream(file);
            }
        }
    }*/

    private static InputStream unwrapEncoding(InputStream attachmentStream, Attachment.Encoding encoding, String sqlcipherKey,
                                              boolean isEncryption)
            throws IOException {
        if(encoding == Attachment.Encoding.Gzip) {
            if(isEncryption) {
                return unwrapEncryption(attachmentStream, sqlcipherKey, true);
            } else {
                //Send input stream with GZIP
                return new GZIPInputStream(attachmentStream);
            }
        } else {
            if(isEncryption) {
                return unwrapEncryption(attachmentStream, sqlcipherKey, false);
            } else {
                //Return file input stream
                return attachmentStream;
            }
        }
    }

    private static InputStream unwrapEncryption(InputStream attachmentStream, String sqlcipherKey,
                                                boolean isGzip) throws IOException {

        //TODO

        //Convert stream to byte array
        byte[] encryptedByteArray = IOUtils.toByteArray(attachmentStream);

        if(isGzip) {
            //SecurityManager.getInstance().decryptAttachmentFileStream()
        } else {
            //SecurityManager.getInstance().decryptAttachmentGzipFileStream()
        }
        //Return decrypted stream
        return null;
    }

    public void setEncryptionStream(Attachment.Encoding encoding, KeyProvider provider) {
        this.encoding = encoding;
    }
}
