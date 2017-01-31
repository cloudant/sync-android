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

import com.cloudant.http.HttpConnection;
import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.internal.common.CouchConstants;
import com.cloudant.sync.internal.mazha.CouchClient;
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
 * {@link RevisionHistoryHelper#addAttachments(Map, Map, boolean, int)}
 * </p>
 *
 * <p>
 * Attachments which are included in subsequent MIME bodies should have <code>follows</code> set to
 * <code>true</code> in the <code>_attachments</code> object.
 * </p>
 *
 * @api_private
 * @see CouchClient#putMultipart(MultipartAttachmentWriter)
 * @see RevisionHistoryHelper#addAttachments(Map, Map, boolean, int)
 * @see
 * <a href target="_blank" =http://couchdb.readthedocs.org/en/latest/api/document/common.html#creating-multiple-attachments>Creating Multiple Attachments</a>
 * @see <a href target="_blank" =http://tools.ietf.org/html/rfc2387>RFC 2387</a>
 */

public class MultipartAttachmentWriter {

    ArrayList<Attachment> attachments;
    byte[] bodyBytes;

    private String boundary;
    private byte partBoundary[];
    private byte trailingBoundary[];
    private static byte crlf[] = "\r\n".getBytes(Charset.forName("UTF-8"));
    private byte contentType[];

    private String id;

    private long contentLength;

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
     *
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
    public void setBody(Map<String, Object> body) {
        this.id = (String) body.get(CouchConstants._id);
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
     *                     Attachment}
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
     *
     * @return The partBoundary
     */
    private String makeBoundary() {
        StringBuffer s = new StringBuffer();
        int length = 32;
        int base = 97; //a..z
        int range = 26;
        while (length-- > 0) {
            char c = (char) (int) (Math.random() * range + base);
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
        return "Multipart/related with " + attachments.size() + " attachments";
    }

    /**
     * Current state representing part of multipart being written (see comment in
     * {@link WriterInputStream#next()} for details)
     */
    private enum State {
        BEGIN,
        BODY,
        BOUNDARY,
        ATTACHMENT,
        TRAILING_BOUNDARY,
        END
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
        return new WriterInputStream();
    }

    /**
     * Utility class to allow the multipart to be accessed as a stream
     *
     * This class keeps track of the current part of the multipart being written using a basic state
     * machine (see comment in {@link #next()} for details)
     */
    private class WriterInputStream extends InputStream {
        int currentAttachment;
        InputStream currentStream;
        State state;

        public WriterInputStream() {
            state = State.BEGIN;
            currentAttachment = 0;
        }

        public int read() throws IOException {
            byte[] b = new byte[1];
            int amountRead = this.read(b);
            // read one byte and return its value or -1 if no bytes were read
            return amountRead == 1 ? b[0] : -1;
        }

        public int read(byte b[]) throws IOException {

            if (state == State.BEGIN) {
                state = State.BODY;
                byte[] bytes = concat(partBoundary, crlf, contentType, crlf, crlf);
                currentStream = new ByteArrayInputStream(bytes);
            }

            int amountRead = 0;
            int howMuch = 0;

            while (currentStream != null) {
                // try to read enough bytes to fill the rest of the bytes array
                howMuch = b.length - amountRead;
                if (howMuch <= 0) {
                    break;
                }
                int read = currentStream.read(b, amountRead, howMuch);
                if (read <= 0) {
                    currentStream.close();
                    currentStream = this.next();
                    continue;
                }
                amountRead += read;
            }

            // signal EOF if we don't have any more
            int retnum = amountRead > 0 ? amountRead : -1;
            return retnum;
        }

        /**
         * Get the next input stream for the current part of the multipart (body / boundary /
         * attachment / trailing boundary)
         *
         * Advance the current state as follows:
         * BEGIN -> BODY -> (BOUNDARY -> ATTACHMENT)+ -> TRAILING_BOUNDARY -> END
         * (where ()+ represents a repeating group of 1..n attachments)
         *
         * After the state reaches END, return null
         *
         * @return The next input stream for the current part of the multipart, or null
         */
        private InputStream next() throws IOException {
            InputStream nextStream;
            if (state == State.BODY) {
                // next state
                state = State.BOUNDARY;
                nextStream = new ByteArrayInputStream(bodyBytes);
            } else if (state == State.BOUNDARY) {
                // next state
                state = State.ATTACHMENT;
                byte[] bytes = concat(crlf, partBoundary, crlf, crlf);
                nextStream = new ByteArrayInputStream(bytes);
            } else if (state == State.ATTACHMENT) {
                // next state
                if (currentAttachment == attachments.size() -1) {
                    // got to the end
                    state = State.TRAILING_BOUNDARY;
                } else {
                    // next attachment
                    state = State.BOUNDARY;
                }
                nextStream = attachments.get(currentAttachment++).getInputStream();
            } else if (state == State.TRAILING_BOUNDARY) {
                // next state
                state = State.END;
                byte[] bytes = concat(crlf, trailingBoundary);
                nextStream = new ByteArrayInputStream(bytes);
            } else if (state == State.END) {
                nextStream = null;
            } else {
                throw new RuntimeException("Unknown state");
            }
            return nextStream;
        }

        /**
         * Utility to concatenate a number of byte arrays
         *
         * @param ins 1 or more byte arrays
         * @return a byte array containing a concatenation of {@code ins}
         */
        private byte[] concat(byte[]... ins) {
            int total = 0;
            for (byte[] in : ins) {
                total += in.length;
            }
            byte[] out = new byte[total];
            int current = 0;
            for (byte[] in : ins) {
                System.arraycopy(in, 0, out, current, in.length);
                current += in.length;
            }
            return out;
        }

    }
}
