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

    MultipartAttachmentWriter(Datastore datastore, DocumentRevision revision, List<Attachment> attachments) throws IOException {

        Map<String, Object> map = revision.getBody().asMap();
        LinkedHashMap<String, Object> atts = new LinkedHashMap<String, Object>();
        // add the attachments entry to the body
        map.put("_attachments", atts);

        // get the ordering right here
        for (Attachment a : attachments) {
            HashMap<String, Object> att = new HashMap<String, Object>();
            atts.put(a.name, att);
            att.put("follows", true);
            att.put("content_type", a.type);
            att.put("length",a.size);
//            att.put("digest","md5-"+new String((new Base64().encode(a.md5))));
        }

        DocumentBody newBody = DocumentBodyFactory.create(map);
        DocumentRevision newRevision = null;
        try {
            newRevision = ((BasicDatastore)datastore).updateDocument(revision.getId(), revision.getRevision(), newBody, false);
        } catch (ConflictException ce) {
            // TODO
            System.out.println("conflict!");
        }

        byte[] bodyBytes = JSONUtils.serializeAsBytes(newRevision.asMap());
        // pick a boundary
        String basicBoundary = this.makeBoundary();
        this.boundary = ("--"+basicBoundary).getBytes();
        this.trailingBoundary = ("--"+basicBoundary+"--").getBytes();
        components = new ArrayList<InputStream>();
        components.add(new ByteArrayInputStream(boundary));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(contentType));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(bodyBytes));
        for (Attachment a : attachments) {
            components.add(new ByteArrayInputStream(crlf));
            components.add(new ByteArrayInputStream(boundary));
            components.add(new ByteArrayInputStream(crlf));
            components.add(new ByteArrayInputStream(crlf));
            components.add(a.getInputStream());
        }
        components.add(new ByteArrayInputStream(crlf));
        components.add(new ByteArrayInputStream(trailingBoundary));
        components.add(new ByteArrayInputStream(crlf));
        currentComponentIdx = 0;
    }

    private byte boundary[];
    private byte trailingBoundary[];
    private byte crlf[] = "\r\n".getBytes();
    private byte contentType[] = "content-type: application/json".getBytes();
    private int currentComponentIdx;

    private ArrayList<InputStream> components;

    private MessageDigest md5;


    public int read() throws java.io.IOException {
        byte[] buf = new byte[1];
        int amountRead = read(buf);
        // will be 0 or EOF
        if (amountRead != 1) {
            return amountRead;
        }
        // return the character we read
        return buf[0];
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
        return amountRead > 0 ? amountRead : -1;
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
}
