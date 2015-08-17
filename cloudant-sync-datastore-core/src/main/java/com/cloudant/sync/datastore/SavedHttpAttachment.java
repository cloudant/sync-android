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

import com.cloudant.android.Base64InputStreamFactory;
import com.cloudant.http.Http;
import com.cloudant.http.HttpConnection;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.zip.GZIPInputStream;


/**
 * Created by rhys on 20/11/14.
 */
public class SavedHttpAttachment extends Attachment {


    private URI attachmentURI;
    private long size;
    private byte[] data;


    /**
     * Creates a SavedHttpAttachment (a fully initialised attachment)
     * with the provided properties
     * @param name The name of the attachment eg bonsai-boston.jpg
     * @param attachmentData The json attachment data from a couchDB instance
     * @param attachmentURI The URI at which the attachment can be downloaded
     * @throws IOException if there is an error decoding the attachment data
     */
    public SavedHttpAttachment(String name, Map<String,Object> attachmentData, URI attachmentURI)
           throws IOException {
         super(name,
                 (String)attachmentData.get("content_type"),
                 Attachment.getEncodingFromString((String)attachmentData.get("encoding")));

         Number length;
         String data = (String)attachmentData.get("data");
         if(data != null){
            byte[] dataArray =  data.getBytes();
            InputStream is = Base64InputStreamFactory.get(new ByteArrayInputStream(dataArray));
            this.data = IOUtils.toByteArray(is);
            length = this.data.length;
         } else {
            length = (Number)attachmentData.get("length");
         }

         this.attachmentURI = attachmentURI;
         this.size = length.longValue();

    }

    @Override
    public InputStream getInputStream() throws IOException {
        if( data == null) {
            HttpConnection connection = Http.GET(attachmentURI);
            if(encoding == Encoding.Gzip) {
                connection.requestProperties.put("Accept-Encoding", "gzip");
                InputStream is = connection.execute().responseAsInputStream();
                return new GZIPInputStream(is);
            } else {
                InputStream is = connection.execute().responseAsInputStream();
                return is;
            }
        } else {
            return new ByteArrayInputStream(data);
        }

    }
}
