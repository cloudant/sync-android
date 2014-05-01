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

    public UnsavedFileAttachment(File file, String type) {
        this.file = file;
        this.name = file.getName();
        this.type = type;
    }

    public UnsavedFileAttachment(File file, String name, String type) {
        this.file = file;
        this.name = name;
        this.type = type;
    }

    public InputStream getInputStream() throws IOException {
        return new FileInputStream(file);
    }

    public long getSize() {
        return file.length();
    }

    private File file;

}
