/*
 * Copyright © 2014, 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.internal.common.CouchConstants;
import com.cloudant.http.HttpConnection;
import com.cloudant.sync.internal.mazha.CouchClient;
import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.internal.util.JSONUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;

/**
 * <p>
 * A class for writing MIME Multipart/Related (RFC 2387) streams to CouchDB.
 * Used to send attachments inline with document revisions without incurring the overhead of base64
 * encoding.
 * </p>
 *
 * <p>
 * Use as follows:
 * </p>
 * <pre>
 * {@code
 * DocumentRevision myDocument;
 * List<Attachment> myAttachments;
 * CouchClient myClient;
 * ...
 * MultipartAttachmentWriter mpw = new MultipartAttachmentWriter();
 * mpw.setRequestBody(myDocument);
 * for (Attachment a : myAttachments) {
 *     mpw.addAttachment(a);
 * }
 * // don't forget to call close when we're done adding attachments
 * mpw.close();
 * // do a PUT to couch
 * myClient.putMultipart(mpw);
 * }
 * </pre>
 *
 * <p>
 * The stream consists of a first MIME body which is the JSON document itself, which needs to have
 * the <code>_attachments</code> object correctly populated. This is currently done by
 * {@link RevisionHistoryHelper#addAttachments(java.util.List, java.util.Map, boolean, int)}
 * </p>
 *
 * <p>
 * Attachments which are included in subsequent MIME bodies should have <code>follows</code> set to
 * <code>true</code> in the <code>_attachments</code> object.
 * </p>
 *
 * @see CouchClient#putMultipart(MultipartAttachmentWriter)
 * @see RevisionHistoryHelper#addAttachments(java.util.List, java.util.Map, boolean, int)
 * @see <a href=http://couchdb.readthedocs.org/en/latest/api/document/common.html#creating-multiple-attachments>Creating Multiple Attachments</a>
 * @see <a href=http://tools.ietf.org/html/rfc2387>RFC 2387</a>
 *
 * @api_private
 */

public class MultipartAttachmentWriter {

    ArrayList<Attachment> attachments;
    byte[] bodyBytes;

    private String boundary;
    private byte partBoundary[];
    private byte trailingBoundary[];
    private static byte crlf[] = "\r\n".getBytes(Charset.forName("UTF-8"));
    private byte contentType[];
    private int currentComponentIdx;

    private ArrayList<InputStream> components;

    private String id;

    private long contentLength;

    // non-null if there was an IOException thrown when calling Attachment.getInputStream()
    private IOException deferrredException;

    /**
     * Construct a <code>MultipartAttachmentWriter</code> with a default <code>boundary</code>
     *
     * @see #makeBoundary()
     */
    public MultipartAttachmentWriter() {
        // auto generate a boundary
        this.boundary = this.makeBoundary();
        this.setup();
    }

    /**
     * Construct a <code>MultipartAttachmentWriter</code> with a specific <code>boundary</code>
     * @param boundary The boundary sequence used to delimit each part. Must not occur anywhere in
     *                 the data of any part.
     */
    public MultipartAttachmentWriter(String boundary) {
        // pick a boundary
        this.boundary = boundary;
        this.setup();
    }

    // common constructor stuff
    private void setup() {
        try {
            this.partBoundary = ("--" + boundary).getBytes("UTF-8");
            this.trailingBoundary = ("--" + boundary + "--").getBytes("UTF-8");
            this.contentType = "content-type: application/json".getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        attachments = new ArrayList<Attachment>();

        // some preamble
        contentLength += partBoundary.length;
        contentLength += 6; // 3 * crlf
        contentLength += contentType.length;

        // although we haven't added it yet, account for the trailing boundary
        contentLength += 2; // crlf
        contentLength += trailingBoundary.length;
    }

    /**
     * Set the JSON body (the first MIME body) for the writer.
     *
     * @param body The DocumentRevision to be serialised as JSON
     */
    public void setBody(Map<String,Object> body) {
        this.id = (String)body.get(CouchConstants._id);
        this.bodyBytes = JSONUtils.serializeAsBytes(body);
        contentLength += bodyBytes.length;
    }

    /**
     * Add an attachment to be streamed as a subsequent MIME body.
     * Depending on the underlying attachment type, (eg file), this is done without loading the
     * entire attachment into memory.
     *
     * @param attachment The attachment to be streamed
     * @param length     Size in bytes of attachment, as it will be transmitted over the network
     *                   (that is, after any encoding)
     * @throws IOException if there was an error getting the input stream from the {@code
     * Attachment}
     */
    public void addAttachment(Attachment attachment, long length) throws IOException {
        this.attachments.add(attachment);
        contentLength += partBoundary.length;
        contentLength += 6; // 3 * crlf
        contentLength += length;
    }

    /**
     * Make a default partBoundary, consisting of 32 random characters in the range [a-z].
     * It is hoped this will be sufficiently random to not appear anywhere in the payload.
     * @return The partBoundary
     */
    private String makeBoundary() {
        StringBuffer s = new StringBuffer();
        int length = 32;
        int base = 97; //a..z
        int range = 26;
        while(length-- > 0) {
            char c = (char)(int)(Math.random()*range+base);
            s.append(c);
        }
        return s.toString();
    }

    /**
     * @return The partBoundary used for this writer
     */
    public String getBoundary() {
        return boundary;
    }

    /**
     * @return The total number of bytes of all headers, bodies and boundaries.
     * For a correctly formed and closed writer, this will be total number of bytes to advertise
     * via the <code>content-length</code> header when performing an HTTP PUT.
     */
    public long getContentLength() {
        return contentLength;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "Multipart/related with "+attachments.size()+" attachments";
    }

    private void makeComponents() {
        components = new ArrayList<InputStream>();

        // preamble
        components.add(new ByteArrayInputStream(partBoundary));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(contentType));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(crlf));

        // body
        components.add(new ByteArrayInputStream(bodyBytes));

        // each attachment
        for (Attachment a : attachments) {
            try {
                components.add(new ByteArrayInputStream(crlf));
                components.add(new ByteArrayInputStream(partBoundary));
                components.add(new ByteArrayInputStream(crlf));
                components.add(new ByteArrayInputStream(crlf));
                components.add(a.getInputStream());
            } catch (IOException ioe) {
                deferrredException = ioe;
            }
        }
        // close
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(trailingBoundary));
    }

    public HttpConnection.InputStreamGenerator makeInputStreamGenerator() {
        return new HttpConnection.InputStreamGenerator() {
            @Override
            public InputStream getInputStream() {
                return makeInputStream();
            }
        };
    }

    public InputStream makeInputStream() {

        this.deferrredException = null;
        currentComponentIdx = 0;
        this.makeComponents();

        return new InputStream() {
            @Override
            /**
             * Read a single byte from the stream.
             * @return The next byte in the stream, expressed as an int in the range [0..255].
             *         If there are no more bytes available, return -1.
             * @throws java.io.IOException if there was an error reading from one of the internal input streams
             *
             */
            public int read() throws java.io.IOException {
                int c;
                do {
                    c = components.get(currentComponentIdx).read();
                    if (c != -1) {
                        return c;
                    } else {
                        ++currentComponentIdx;
                    }
                } while (currentComponentIdx < components.size());
                // we got through all the components, end of stream
                return -1;
            }

            /**
             * Read at most the next <code>bytes.length</code> bytes from the stream.
             *
             * @param bytes A buffer to read the next <i>n</i> bytes into, up to the
             *              limit of the length of the buffer, or the number of bytes
             *              available, whichever is lower.
             * @return The actual number of bytes read, or -1 to signal the end of the
             * stream has been reached.
             * @throws java.io.IOException if there was an error reading from one of the
             *                             internal input streams
             */
            @Override
            public int read(byte[] bytes) throws java.io.IOException {
                if (deferrredException != null) {
                    // there was an exception caught when calling Attachment.getInputStream()
                    // - throw it now
                    throw deferrredException;
                }
                int amountRead = 0;
                int currentOffset = 0;
                int howMuch = 0;
                do {
                    InputStream currentComponent = components.get(currentComponentIdx);
                    // try to read enough bytes to fill the rest of the bytes array
                    howMuch = bytes.length - currentOffset;
                    if (howMuch <= 0) {
                        break;
                    }
                    int read = currentComponent.read(bytes, currentOffset, howMuch);
                    if (read <= 0) {
                        currentComponentIdx++;
                        continue;
                    }
                    amountRead += read;
                    currentOffset += howMuch;
                } while (currentComponentIdx < components.size());

                // signal EOF if we don't have any more
                int retnum = amountRead > 0 ? amountRead : -1;
                return retnum;

            }
        };
    }
}
