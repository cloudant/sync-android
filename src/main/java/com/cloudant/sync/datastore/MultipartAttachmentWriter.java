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

import com.cloudant.sync.util.JSONUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

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
 * mpw.setBody(myDocument);
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
 * {@link com.cloudant.sync.datastore.RevisionHistoryHelper#addAttachments(java.util.List, DocumentRevision, java.util.Map, com.cloudant.sync.replication.PushAttachmentsInline)}
 * </p>
 *
 * <p>
 * Attachments which are included in subsequent MIME bodies should have <code>follows</code> set to
 * <code>true</code> in the <code>_attachments</code> object.
 * </p>
 *
 * @see com.cloudant.mazha.CouchClient#putMultipart(MultipartAttachmentWriter)
 * @see com.cloudant.sync.datastore.RevisionHistoryHelper#addAttachments(java.util.List, DocumentRevision, java.util.Map, com.cloudant.sync.replication.PushAttachmentsInline)
 * @see <a href=http://couchdb.readthedocs.org/en/latest/api/document/common.html#creating-multiple-attachments>Creating Multiple Attachments</a>
 * @see <a href=http://tools.ietf.org/html/rfc2387>RFC 2387</a>
 *
 */

public class MultipartAttachmentWriter extends InputStream {

    /**
     * Construct a <code>MultipartAttachmentWriter</code> with a default <code>partBoundary</code>
     *
     * @see #makeBoundary()
     */
    public MultipartAttachmentWriter() {
        // pick a partBoundary
        this.boundary = this.makeBoundary();
        this.partBoundary = ("--"+boundary).getBytes();
        this.trailingBoundary = ("--"+boundary+"--").getBytes();

        components = new ArrayList<InputStream>();

        // some preamble
        contentLength += partBoundary.length;
        contentLength += 6; // 3 * crlf
        contentLength += contentType.length;

        components.add(new ByteArrayInputStream(partBoundary));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(contentType));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(crlf));
    }

    /**
     * Set the JSON body (the first MIME body) for the writer.
     *
     * @param body The DocumentRevision to be serialised as JSON
     */
    public void setBody(DocumentRevision body) {
        byte[] bodyBytes = JSONUtils.serializeAsBytes(body.asMap());

        contentLength += bodyBytes.length;

        components.add(new ByteArrayInputStream(bodyBytes));
        this.id = body.getId();
        this.revision = body.getRevision();
    }

    /**
     * Add an attachment to be streamed as a subsequent MIME body.
     * Depending on the underlying attachment type, (eg file), this is done without loading the
     * entire attachment into memory.
     *
     * @param attachment The attachment to be streamed
     * @throws IOException
     */
    public void addAttachment(Attachment attachment) throws IOException {
        contentLength += partBoundary.length;
        contentLength += 6; // 3 * crlf
        contentLength += attachment.getSize();

        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(partBoundary));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(crlf));
        components.add(attachment.getInputStream());
    }

    /**
     * Add the trailing partBoundary to signify that all of the attachments are present.
     * This <b>must</b> be called before any client attempts to issue a read.
     */
    public void close() {
        contentLength += trailingBoundary.length;
        contentLength += 2; // crlf

        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(trailingBoundary));
        currentComponentIdx = 0;
    }

    private String boundary;
    private byte partBoundary[];
    private byte trailingBoundary[];
    private static byte crlf[] = "\r\n".getBytes();
    private static byte contentType[] = "content-type: application/json".getBytes();
    private int currentComponentIdx;

    private ArrayList<InputStream> components;

    private String id;
    private String revision;

    private long contentLength;

    /**
     * Read a single byte from the stream.
     * @return The next byte in the stream, expressed as an int in the range [0..255].
     *         If there are no more bytes available, return -1.
     * @throws java.io.IOException
     */
    public int read() throws java.io.IOException {

        byte[] buf = new byte[1];
        int amountRead = read(buf);

        // read(byte[]) can return 0 if there are no bytes available or -1 to signal EOF
        // in either case, return -1 to indicate we are done
        if (amountRead != 1) {
            return -1;
        }
        // return the character we read
        // convert from 2s complement
        int c = buf[0];
        if (c < 0) {
            return c + 256;
        } else {
            return c;
        }
    }

    /**
     * Read at most the next <code>bytes.length</code> bytes from the stream.
     * @param bytes A buffer to read the next <i>n</i> bytes into, up to the limit of the length of the buffer, or the number of bytes available, whichever is lower.
     * @return The actual number of bytes read, or -1 to signal the end of the stream has been reached.
     * @throws java.io.IOException
     */
    @Override
    public int read(byte[] bytes) throws java.io.IOException {
        int amountRead = 0;
        int currentOffset = 0;
        int howMuch = 0;
        do {
            InputStream currentComponent = components.get(currentComponentIdx);
            if (currentComponent.available() == 0) {
                ++currentComponentIdx;
                continue;
            }
            howMuch = Math.min(bytes.length - currentOffset, currentComponent.available());
            amountRead += currentComponent.read(bytes, currentOffset, howMuch);
            currentOffset += howMuch;
        } while (currentComponentIdx < components.size()-1 && howMuch > 0);

        // signal EOF if we don't have any more
        int retnum =  amountRead > 0 ? amountRead : -1;
        return retnum;

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
    public String getRevision() {
        return revision;
    }

    @Override
    public String toString() {
        return "Multipart/related with "+components.size()+" components";
    }
}
