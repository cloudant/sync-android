package com.cloudant.sync.sqlite.android.encryption;

import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.KeyUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Created by estebanmlaver on 5/12/15.
 */
public class AttachmentStreamFactory {

    private static final int ANDROID_BUFFER_8K = 8192;

    private static AttachmentStreamFactory singleton;

    public AttachmentStreamFactory(){ }


    public static InputStream getStream(Attachment.Encoding encoding, File file, KeyProvider provider) throws IOException {
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
    }

    private static InputStream unwrapEncoding(Attachment.Encoding encoding, String sqlcipherKey, File file, boolean isEncryption)
            throws IOException {
        if(encoding == Attachment.Encoding.Gzip) {
            if(isEncryption) {
                return unwrapEncryption(sqlcipherKey, file, true);
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
    }

    private static InputStream unwrapEncryption(String sqlcipherKey, File file, boolean isGzip) throws IOException {

        //TODO
        if(isGzip) {
            //SecurityManager.getInstance().decryptAttachmentFileStream()
        } else {
            //SecurityManager.getInstance().decryptAttachmentGzipFileStream()
        }
        //Return decrypted stream
        return null;
    }
}
