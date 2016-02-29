/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

import com.cloudant.android.Base64OutputStreamFactory;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.common.TestOptions;
import com.cloudant.mazha.Response;
import com.cloudant.sync.replication.ReplicationTestBase;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tomblench on 15/01/15.
 */

// Tests for the DocumentRevisionBuilder which require a running CouchDB
// here we mostly (ab)use CouchDB as a remote http source of attachment data
// for the sake of building up a local document with attachments
@Category(RequireRunningCouchDB.class)
public class DocumentRevisionBuilderAttachmentsTest extends ReplicationTestBase {

    Map<String,Object> documentRev;
    Map<String, String>body;
    URI documentURI;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        documentRev  = new HashMap<String,Object>();
        documentRev.put("_id","someIdHere");
        documentRev.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        documentRev.put("aKey","aValue");
        documentRev.put("hello","world");

        body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        documentURI =  new URI(remoteDb.getCouchClient().getRootUri().toString()+"/someIdHere");
    }

    @Test
    public void buildRevisionFromMapValidMapWithAttachments() throws Exception {

        //lets the get the attachment encoded

        File file = TestUtils.loadFixture("fixture/bonsai-boston.jpg");

        byte[] unencodedAttachment = FileUtils.readFileToByteArray(file);
        byte[] encodedAttachment = this.encodeAttachment(unencodedAttachment);

        Map<String,Map<String,Object>> attachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> bonsai = createAttachmentMap(encodedAttachment,"image/jpeg",false);

        attachments.put("bonsai-boston.jpg",bonsai);
        documentRev.put("_attachments", attachments);


        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(documentURI,documentRev);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body, revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        ByteArrayInputStream expected = new ByteArrayInputStream(unencodedAttachment);
        InputStream actual = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        Assert.assertTrue(TestUtils.streamsEqual(expected,actual));


    }

    @Test
    public void buildRevisionFromMapValidMapWithAttachmentsDataExcluded() throws Exception {

        Assume.assumeFalse("Not running test as 'buildRevisionFromMap' can't retrieve the " +
                "attachment with cookie authentication enabled", TestOptions.COOKIE_AUTH);

        //lets the get the attachment encoded

        File file = TestUtils.loadFixture("fixture/bonsai-boston.jpg");

        byte[] unencodedAttachment = FileUtils.readFileToByteArray(file);
        byte[] encodedAttachment = this.encodeAttachment(unencodedAttachment);

        //create a revision on a couchDB instance so we can test the download
        //of attachments
        Map<String,Object> remoteDoc = new HashMap<String,Object>();
        remoteDoc.put("_id","someIdHere");
        Map<String,Map<String,Object>> remoteAttachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> remoteBonsai = createAttachmentMap(encodedAttachment,"image/jpeg",false);

        remoteAttachments.put("bonsai-boston.jpg",remoteBonsai);
        remoteDoc.put("_attachments",remoteAttachments);

        remoteDb.create(remoteDoc);


        //build up the test json map

        Map<String,Map<String,Object>> attachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> bonsai = createAttachmentMap(encodedAttachment,"image/jpeg",true);

        attachments.put("bonsai-boston.jpg",bonsai);
        documentRev.put("_attachments",attachments);

        URI uri =  new URI(remoteDb.getCouchClient().getRootUri().toString()+"/"+"someIdHere");

        //create the document revision and test

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,documentRev);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body, revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        InputStream attachmentInputStream = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        InputStream expectedInputStream = new ByteArrayInputStream(unencodedAttachment);
        Assert.assertTrue(TestUtils.streamsEqual(expectedInputStream, attachmentInputStream));

    }

    @Test
    public void buildRevisionFromMapValidMapWithAttachmentsDataExcludedNonWinningRev() throws Exception {

        Assume.assumeFalse("Not running test as 'buildRevisionFromMap' can't retrieve the " +
                "attachment with cookie authentication enabled", TestOptions.COOKIE_AUTH);

        //lets the get the attachment encoded

        File file = TestUtils.loadFixture("fixture/bonsai-boston.jpg");

        byte[] unencodedAttachment = FileUtils.readFileToByteArray(file);
        byte[] encodedAttachment = this.encodeAttachment(unencodedAttachment);

        //create a revision on a couchDB instance so we can test the download
        //of attachments
        Map<String,Object> remoteDoc = new HashMap<String,Object>();
        remoteDoc.put("_id","someIdHere");
        Map<String,Map<String,Object>> remoteAttachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> remoteBonsai = createAttachmentMap(encodedAttachment,"image/jpeg",false);

        remoteAttachments.put("bonsai-boston.jpg",remoteBonsai);
        remoteDoc.put("_attachments",remoteAttachments);


        Response response = remoteDb.create(remoteDoc);
        remoteDoc.put("_rev",response.getRev());

        remoteDoc.remove("_attachments");
        remoteDb.update("someIdHere",remoteDoc);


        //build up the test json map

        Map<String,Map<String,Object>> attachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> bonsai = this.createAttachmentMap(encodedAttachment,"image/jpeg",true);

        attachments.put("bonsai-boston.jpg",bonsai);
        documentRev.put("_attachments",attachments);

        URI uri =  new URI(remoteDb.getCouchClient().getRootUri().toString()+"/"+"someIdHere"+"?rev="+response.getRev());

        //create the document revision and test

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,documentRev);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body, revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        InputStream attachmentInputStream = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        InputStream expectedInputStream = new ByteArrayInputStream(unencodedAttachment);
        Assert.assertTrue(TestUtils.streamsEqual(expectedInputStream, attachmentInputStream));

    }

    @Test
    public void buildRevisionFromMapValidMapWithTextAttachmentsDataExcluded() throws Exception {

        Assume.assumeFalse("Not running test as 'buildRevisionFromMap' can't retrieve the " +
                "attachment with cookie authentication enabled", TestOptions.COOKIE_AUTH);

        //lets the get the attachment encoded

        String attachmentText = "Hello World";

        byte[] unencodedAttachment = attachmentText.getBytes();
        byte[] encodedAttachment = this.encodeAttachment(unencodedAttachment);

        //create a revision on a couchDB instance so we can test the download
        //of attachments
        Map<String,Object> remoteDoc = new HashMap<String,Object>();
        remoteDoc.put("_id","someIdHere");
        Map<String,Map<String,Object>> remoteAttachments = new HashMap<String,Map<String,Object>>();

        remoteAttachments.put("hello.txt",this.createAttachmentMap(encodedAttachment,"text/plain",false));
        remoteDoc.put("_attachments",remoteAttachments);

        remoteDb.create(remoteDoc);
        documentURI = new URI(remoteDb.getCouchClient().getRootUri().toString()+"/"+"someIdHere");

        Map<String,Map<String,Object>> attachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> bonsai = createAttachmentMap(encodedAttachment,"text/plain",true);//new HashMap<String,Object>();
        bonsai.put("encoding","gzip");

        attachments.put("hello.txt",bonsai);
        documentRev.put("_attachments",attachments);

        //create the document revision and test

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(documentURI,documentRev);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body, revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        InputStream attachmentInputStream = revision.getAttachments().get("hello.txt").getInputStream();
        InputStream expectedInputStream = new ByteArrayInputStream(unencodedAttachment);
        Assert.assertTrue(TestUtils.streamsEqual(expectedInputStream, attachmentInputStream));

    }

    private Map<String,Object> createAttachmentMap(byte[] encodedAttachment,String contentType,boolean stub){
        String encodedAttachmentString = new String(encodedAttachment);

        Map<String,Object> remoteBonsai = new HashMap<String,Object>();
        remoteBonsai.put("digest","thisisahasiswear");
        remoteBonsai.put("revpos",1);
        remoteBonsai.put("content_type",contentType);
        if(stub) {
            remoteBonsai.put("stub", stub);
            remoteBonsai.put("length",encodedAttachmentString.length());

        } else  {
            remoteBonsai.put("data", encodedAttachmentString);
        }


        return remoteBonsai;
    }

    private byte[] encodeAttachment(byte[] unencodedAttachment) throws Exception {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream out = Base64OutputStreamFactory.get(byteArrayOutputStream);
        out.write(unencodedAttachment);

        //close and flush all streams
        out.flush();
        byteArrayOutputStream.flush();
        out.close();
        byteArrayOutputStream.close();

        return byteArrayOutputStream.toByteArray();
    }



}
