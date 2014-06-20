/**
 * Copyright (c) 2014 Cloudant, Inc. All rights reserved.
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

import com.cloudant.sync.util.Misc;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * An attachment which has been been copied to a temporary location and had its sha1 calculated,
 * prior to being added to the datastore.
 *
 * In most cases, this class will only be used by the AttachmentManager and BasicDatastore classes.
 */
public class PreparedAttachment {

    /**
     * Prepare an attachment by copying it to a temp location and calculating its sha1.
     *
     * @param attachment The attachment to prepare
     * @param attachmentsDir The 'BLOB store' or location where attachments are stored for this database
     * @throws IOException
     */
    public PreparedAttachment(Attachment attachment,
                              String attachmentsDir) throws IOException {
        this.attachment = attachment;
        this.tempFile = new File(attachmentsDir, "temp" + UUID.randomUUID());
        FileUtils.copyInputStreamToFile(attachment.getInputStream(), tempFile);
        this.sha1 = Misc.getSha1(new FileInputStream(tempFile));
    }

    public final Attachment attachment;
    public final File tempFile;
    public final byte[] sha1;
}

