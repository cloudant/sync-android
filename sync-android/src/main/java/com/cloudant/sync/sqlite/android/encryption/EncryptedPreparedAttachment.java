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
import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.AttachmentNotSavedException;
import com.cloudant.sync.util.Misc;

import java.io.File;
import java.util.UUID;

/**
 * Created by estebanmlaver.
 */
public class EncryptedPreparedAttachment {

    private final Attachment attachment;
    private final File tempFile;
//    private final byte[] sha1;

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
            // TODO Encryption needs to happen
            //Get SHA1 before encryption
//            this.sha1 = Misc.getSha1(new FileInputStream(tempFile));
            //Encrypt file attachment
//            EncryptionInputStreamUtils.copyInputStreamToEncryptedFile(attachment.getInputStream(),tempFile,sqlcipherKey);
//        } catch (IOException e){
//            throw new AttachmentNotSavedException(e);
        } catch (Exception e) {
            //Exception clause added for copyInputStreamToEncryptedFile
            throw new AttachmentNotSavedException(e);
        }
    }

}
