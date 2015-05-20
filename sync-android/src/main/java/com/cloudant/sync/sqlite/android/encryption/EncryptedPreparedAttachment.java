package com.cloudant.sync.sqlite.android.encryption;

import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.AttachmentNotSavedException;
import com.cloudant.sync.util.Misc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * Created by estebanmlaver.
 */
public class EncryptedPreparedAttachment {

    private final Attachment attachment;
    private final File tempFile;
    private final byte[] sha1;

    /**
     * With encryption, prepare an attachment by copying it to a temp location and calculating its sha1.
     *
     * @param attachment The attachment to prepare
     * @param attachmentsDir The 'BLOB store' or location where attachments are stored for this database
     * @throws AttachmentNotSavedException
     */
    public EncryptedPreparedAttachment(Attachment attachment,
                                       String attachmentsDir, String sqlcipherKey) throws AttachmentException {
        this.attachment = attachment;
        this.tempFile = new File(attachmentsDir, "temp" + UUID.randomUUID());
        try {
            //Get SHA1 before encryption
            this.sha1 = Misc.getSha1(new FileInputStream(tempFile));
            //Encrypt file attachment
            EncryptionFileUtils.copyInputStreamToEncryptedFile(attachment.getInputStream(),tempFile,sqlcipherKey);
        } catch (IOException e){
            throw new AttachmentNotSavedException(e);
        } catch (Exception e) {
            //Exception clause added for copyInputStreamToEncryptedFile
            throw new AttachmentNotSavedException(e);
        }
    }

}
