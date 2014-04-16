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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by tomblench on 11/03/2014.
 */

/**
 * An Attachment which has been prepared by the user for saving to the database
 */
public class UnsavedFileAttachment extends Attachment {

    protected UnsavedFileAttachment(File file, String type) {
        this.name = file.getName();
        this.file = file;
        this.type = type;
    }

    public UnsavedFileAttachment(InputStream inputStream, String name, String type) {
        this.name = name;
        this.inputStream = inputStream;
        this.type = type;
    }

    public InputStream getInputStream() throws IOException {
        // if inputStream not set, then we must be streaming from file
        if (inputStream == null) {
            return new FileInputStream(file);
        } else {
            return inputStream;
        }
    }

    private File file;
    private InputStream inputStream;
}
