/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.documentstore;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by tomblench on 11/03/2014.
 */

/**
 * <p>
 *   An <a target="_blank" href="http://docs.couchdb.org/en/2.0.0/intro/api.html#attachments">
 *   attachment</a> associated with a CouchDB document.
 * </p>
 * <p>
 *   An attachment is identified by a name and has other metadata associated with it such as MIME
 *   type and encoding.
 * </p>
 *
 * @api_public
 */
public abstract class Attachment implements Comparable<Attachment>{

    public Attachment(String name, String type, Encoding encoding) {
        this.name = name;
        this.type = type;
        this.encoding = encoding;
    }

    /**
     * Name of the attachment, must be unique for a given document
     */
    public final String name;

    /**
     * MIME type of the attachment
     */
    public final String type;

    /**
     * <p>
     *   Encoding - Plain or GZIP
     * </p>
     * <p>
     *   NB: If the encoding is GZIP, {@link #getInputStream()} will automatically decompress the
     *   stream
     * </p>
     */
    public final Encoding encoding;
    
    /**
     * <p>
     *   Get contents of attachments as a stream.
     * </p>
     * <p>
     *   NB: If the {@link #encoding} encoding is GZIP the stream will be automatically decompressed
     * </p>
     * <p>
     *   Caller must call close() when done.
     * </p>
     * @return contents of attachments as a stream
     * @throws IOException if there was an error obtaining the stream, eg from disk or network
     */     
    public abstract InputStream getInputStream() throws IOException;

    @Override
    public String toString() {
        return "Attachment: "+name+", type: "+type;
    }

    @Override
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Attachment that = (Attachment) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }
        return encoding == that.encoding;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (encoding != null ? encoding.hashCode() : 0);
        return result;
    }
}
