package com.cloudant.sync.sqlite.android.encryption;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.InputStream;

/**
 * Created by estebanmlaver.
 */
public class EncryptionFileUtils extends FileUtils {

    public static void copyInputStreamToEncryptedFile(InputStream decryptedStream, File encryptedDestinationFile, String sqlcipherKey) throws Exception {
        byte[] sourceInBytes = IOUtils.toByteArray(decryptedStream);

        SecurityManager.getInstance().encryptAttachment(sqlcipherKey,
                null, encryptedDestinationFile, sourceInBytes);
    }

    public static InputStream decryptInputStream(InputStream encryptedStream, File attachmentFile, String sqlcipherKey) throws Exception {

        return SecurityManager.getInstance().decryptAttachmentFileStream(sqlcipherKey,attachmentFile.getName(),attachmentFile);

    }
}
