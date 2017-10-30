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
 *   An <a target="_blank" href="http://docs.couchdb.org/en/latest/intro/api.html#attachments">
 *   attachment</a> associated with a CouchDB document.
 * </p>
 * <p>
 *   An attachment is identified by a name and has other metadata associated with it such as MIME
 *   type and encoding.
 * </p>
 */
public abstract class Attachment {

    protected Attachment(String type, Encoding encoding, long length) {
        this.type = type;
        this.encoding = encoding;
        this.length = length;
    }

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
     * The length, in bytes, of the stream returned by {@link #getInputStream()} or -1 if the length
     * is unknown (for instance if the data is backed by a stream)
     */
    public final long length;
    
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

    /**
     * <p>
     *   Returns the {@link #encoding} encoding using the string in the attachment's JSON metadata.
     * </p>
     * @param encodingString the string containing the attachment's {@link #encoding} encoding.
     * @return the attachment's {@link #encoding} encoding.
     */
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
