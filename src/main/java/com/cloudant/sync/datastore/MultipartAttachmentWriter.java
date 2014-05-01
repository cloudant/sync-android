package com.cloudant.sync.datastore;

import com.cloudant.sync.util.JSONUtils;

import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tomblench on 24/02/2014.
 */

// DocumentRevision setAttachments(DocumentRevision rev, List<Attachment> attachments);

/*
* Take a revision:
* - It has had setAttachments called on it already
* - With _attachments pre-populated
* - Each attachment has content_type, length, digest
* - Go thru the attachments dictionary in order
* - Get the attachment out of the blob store
*
*/



public class MultipartAttachmentWriter extends InputStream {

    public MultipartAttachmentWriter() {
        // pick a boundary
        this.basicBoundary = this.makeBoundary();
        this.boundary = ("--"+basicBoundary).getBytes();
        this.trailingBoundary = ("--"+basicBoundary+"--").getBytes();
        components = new ArrayList<InputStream>();
        // some preamble

        contentLength += boundary.length;
        contentLength += 6;
        contentLength += contentType.length;

        components.add(new ByteArrayInputStream(boundary));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(contentType));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(crlf));
    }

    public void setBody(DocumentRevision body) {


        byte[] bodyBytes = JSONUtils.serializeAsBytes(body.asMap());

        contentLength += bodyBytes.length;

        components.add(new ByteArrayInputStream(bodyBytes));
        this.id = body.getId();
        this.revision = body.getRevision();
    }

    public void addAttachment(Attachment attachment) throws IOException {

        contentLength += boundary.length;
        contentLength += 6;
        contentLength += attachment.getSize();

        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(boundary));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(crlf));
        components.add(attachment.getInputStream());
    }

    public void close() {

        contentLength += trailingBoundary.length;
        contentLength += 2;

        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(trailingBoundary));
        currentComponentIdx = 0;
    }

    private String basicBoundary;
    private byte boundary[];
    private byte trailingBoundary[];
    private byte crlf[] = "\r\n".getBytes();
    private byte contentType[] = "content-type: application/json".getBytes();
    private int currentComponentIdx;

    private ArrayList<InputStream> components;

    private MessageDigest md5;

    private String id;
    private String revision;

    private long contentLength;

    public int read() throws java.io.IOException {

        byte[] buf = new byte[1];
        int amountRead = read(buf);

        // will be 0 or EOF
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

    public String getBoundary() {
        return basicBoundary;
    }

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
