package com.cloudant.sync.sqlite.android.encryption;

import com.cloudant.sync.datastore.UnsavedFileAttachment;
import com.cloudant.sync.datastore.encryption.KeyProvider;

import java.io.File;

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
    }


    public EncryptedUnsavedFileAttachment(File file, String type, KeyProvider
            provider) {
        super(file, type);
        this.file = file;
        //Encrypt attachment
        //SecurityManager.getInstance().encryptAttachment();
    }
}
