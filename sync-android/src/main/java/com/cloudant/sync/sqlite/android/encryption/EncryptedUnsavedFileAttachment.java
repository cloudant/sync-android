package com.cloudant.sync.sqlite.android.encryption;

import com.cloudant.sync.datastore.UnsavedFileAttachment;
import com.cloudant.sync.datastore.encryption.KeyProvider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by estebanmlaver.
 */
public class EncryptedUnsavedFileAttachment extends UnsavedFileAttachment {

    private File file;

    public EncryptedUnsavedFileAttachment(File file, String type, Encoding encoding, KeyProvider
            provider) {
        super(file, type, encoding);
        this.file = file;
        //Encrypt attachment
        //SecurityManager.getInstance().encryptAttachment();

        //Set encoding and key provider for stream
        //AttachmentStreamFactory.getInstance().setEncryptionStream(encoding, provider);

    }


    public EncryptedUnsavedFileAttachment(File file, String type, KeyProvider
            provider) throws Exception {
        super(file, type);
        this.file = file;
        //Encrypt attachment
        //SecurityManager.getInstance().encryptAttachment();
        //SecurityManager.getInstance().encryptAttachment(
        //        KeyUtils.sqlCipherKeyForKeyProvider(provider),type,this.name);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        //AttachmentStreamFactory.getEncryptedStream(super.getInputStream());
        return null;
    }
}
