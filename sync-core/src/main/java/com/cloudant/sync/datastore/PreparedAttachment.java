/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An attachment which has been been copied to a temporary location and had its sha1 calculated,
 * prior to being added to the datastore.
 *
 * In most cases, this class will only be used by the AttachmentManager and BasicDatastore classes.
 */
public class PreparedAttachment {

    private Logger logger = Logger.getLogger(PreparedAttachment.class.getCanonicalName());

    public final Attachment attachment;
    public final File tempFile;
    public final byte[] sha1;
    public final long length;
    public final long encodedLength;
    
    /**
     * Prepare an attachment by copying it to a temp location and calculating its sha1.
     *
     * @param attachment The attachment to prepare
     * @param attachmentsDir The 'BLOB store' or location where
     * attachments are stored for this database
     * @param length Length in bytes, before any encoding. This
     * argument is ignored if the attachment is not encoded
     * @param attachmentStreamFactory The AttachmentStreamFactory for
     * intantiating attachment input and output streams
     * @throws AttachmentNotSavedException
     */
    public PreparedAttachment(Attachment attachment,
                              String attachmentsDir,
                              long length,
                              AttachmentStreamFactory attachmentStreamFactory) throws AttachmentException {
        this.attachment = attachment;
        this.tempFile = new File(attachmentsDir, "temp" + UUID.randomUUID());
        InputStream attachmentInStream = null;
        OutputStream tempFileOutStream = null;
        MessageDigest calculateSha1 = null;
        int totalRead = 0;
        try {
            attachmentInStream = attachment.getInputStream();

            tempFileOutStream = attachmentStreamFactory.getOutputStream(this.tempFile,
                    Attachment.Encoding.Plain);

            calculateSha1 = MessageDigest.getInstance("SHA-1");
            int bufferSize = 1024;
            byte[] buffer = new byte[bufferSize];
            int bytesRead;
            while ((bytesRead = attachmentInStream.read(buffer)) != -1) {
                calculateSha1.update(buffer, 0, bytesRead);
                tempFileOutStream.write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "Problem reading from input or writing to output stream ", e);
            throw new AttachmentNotSavedException(e);
        } catch (NoSuchAlgorithmException e) {
            logger.log(Level.WARNING, "Problem calculating SHA1 for attachment stream ", e);
            throw new AttachmentNotSavedException(e);
        } finally {
            //Ensure the attachment input stream and file output stream is closed after calculating the hash
            IOUtils.closeQuietly(attachmentInStream);
            IOUtils.closeQuietly(tempFileOutStream);
        }
        
        //Set attachment length from bytes read in input stream
        if (this.attachment.encoding == Attachment.Encoding.Plain) {
            this.length = totalRead;
            // 0 signals "no encoded length" - this is consistent with couch which does not send
            // encoded_length if the encoding is "plain"
            this.encodedLength = 0;
        } else {
            // the pre-encoded length is known, so store it
            this.length = length;
            this.encodedLength = totalRead;
        }

        this.sha1 = calculateSha1.digest();
    }

}
