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

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by tomblench on 11/03/2014.
 */

/**
 * Base class for Attachments
 */
public abstract class Attachment implements Comparable<Attachment>{

    public Attachment(String name, String type, Encoding encoding) {
        this.name = name;
        this.type = type;
        this.encoding = encoding;
    }

    /**
     * Name of the attachment, must be unique for a given revision
     */
    public final String name;

    /**
     * MIME type of the attachment
     */
    public final String type;

    /**
     * Encoding - Plain or GZIP
     */
    public final Encoding encoding;

    /**
     * Size in bytes, may be -1 if not known (e.g., HTTP URL for new attachment)
     */
    public abstract long getSize();
    
    /**
     * Gets contents of attachments as a stream.
     *
     * Caller must call close() when done.
     */     
    public abstract InputStream getInputStream() throws IOException;

    public String toString() {
        return "Attachment: "+name+", type: "+type;
    }

    public int compareTo(Attachment other) {
        return name.compareTo(other.name);
    }

    public enum Encoding {
        /**
         * Plain encoding: the contents are not compressed.
         */
        Plain,
        /**
         * GZIP encoding: the contents are compressed using GZIP compression.
         */
        Gzip
    }

    public static Encoding getEncodingFromString(String encodingString){
        if(encodingString == null || encodingString.isEmpty() || encodingString.equalsIgnoreCase("Plain")){
            return Encoding.Plain;
        } else if(encodingString.equalsIgnoreCase("gzip")){
            return Encoding.Gzip;
        } else {
            throw new IllegalArgumentException("Unsupported encoding");
        }
    }

}
