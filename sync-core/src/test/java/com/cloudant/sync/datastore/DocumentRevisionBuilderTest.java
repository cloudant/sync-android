package com.cloudant.sync.datastore;

import com.cloudant.android.Base64OutputStreamFactory;
import com.cloudant.mazha.Response;
import com.cloudant.sync.replication.ReplicationTestBase;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DocumentRevisionBuilderTest extends ReplicationTestBase {

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
    public void buildRevisionFromMapValidMap() throws Exception {

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(documentURI, documentRev);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");

    }

    @Test
    public void buildsMutableRevision(){
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setBody(DocumentBodyFactory.create(body));
        builder.setDocId("someIdHere");
        builder.setRevId("3-750dac460a6cc41e6999f8943b8e603e");

        MutableDocumentRevision revision = builder.buildMutable();
        Assert.assertEquals("someIdHere",revision.docId);
        Assert.assertEquals(body,revision.body.asMap());
        Assert.assertEquals("3-750dac460a6cc41e6999f8943b8e603e", revision.getSourceRevisionId());
        Assert.assertNull(revision.getRevision());
    }

    @Test
    public void buildMutableRevisionMissingRevId() {
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setBody(DocumentBodyFactory.create(body));
        builder.setDocId("someIdHere");

        MutableDocumentRevision revision = builder.buildMutable();
        Assert.assertEquals("someIdHere",revision.docId);
        Assert.assertEquals(body,revision.body.asMap());
        Assert.assertNull(revision.getSourceRevisionId());
        Assert.assertNull(revision.getRevision());
    }

    @Test
    public void buildMutableRevisionMissingDocId() {
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setBody(DocumentBodyFactory.create(body));
        builder.setRevId("3-750dac460a6cc41e6999f8943b8e603e");

        MutableDocumentRevision revision = builder.buildMutable();
        Assert.assertNull(revision.docId);
        Assert.assertEquals(body,revision.body.asMap());
        Assert.assertEquals("3-750dac460a6cc41e6999f8943b8e603e", revision.getSourceRevisionId());
        Assert.assertNull(revision.getRevision());
    }

    @Test
    public void buildRevisionFromMapValidMapDelectedDoc() throws Exception {

        documentRev.put("_deleted", Boolean.TRUE);

        BasicDocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(documentURI, documentRev);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertTrue(revision.isDeleted());

    }

    @Test
    public void buildRevisionFromMapValidMapAllFields() throws Exception {

        //add missing valid _ prefixed keys to map
        documentRev.put("_attachments",new HashMap<String,String>());
        documentRev.put("_conflicts",new String[0]);
        documentRev.put("_deleted_conflicts",new HashMap<String,Object>());
        documentRev.put("_local_seq",1);
        documentRev.put("_revs_info",new HashMap<String,Object>());
        documentRev.put("_revisions",new String[0]);

        BasicDocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(documentURI,documentRev);

        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),0);

    }

    @Test(expected = IllegalArgumentException.class)
    public void buildRevisionFromMapInValidMap() throws Exception {
        documentRev.put("_notValidKey","not valid");

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(documentURI,documentRev);

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
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        ByteArrayInputStream expected = new ByteArrayInputStream(unencodedAttachment);
        InputStream actual = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        Assert.assertTrue(TestUtils.streamsEqual(expected,actual));


    }

    @Test
    public void buildRevisionFromMapValidMapWithAttachmentsDataExcluded() throws Exception {

        //lets the get the attachment encoded

        File file = TestUtils.loadFixture("fixture/bonsai-boston.jpg");

        byte[] unencodedAttachment = FileUtils.readFileToByteArray(file);
        byte[] encodedAttachment = this.encodeAttachment(unencodedAttachment);

        String encodedAttachmentString = new String(encodedAttachment);

        //create a revision on a couchDB instance so we can test the download
        //of attachments
        Map<String,Object> remoteDoc = new HashMap<String,Object>();
        remoteDoc.put("_id","someIdHere");
        Map<String,Map<String,Object>> remoteAttachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> remoteBonsai = createAttachmentMap(encodedAttachment,"image/jpeg",false);//new HashMap<String,Object>();

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
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        InputStream attachmentInputStream = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        InputStream expectedInputStream = new ByteArrayInputStream(unencodedAttachment);
        Assert.assertTrue(TestUtils.streamsEqual(expectedInputStream, attachmentInputStream));

    }

    @Test
    public void buildRevisionFromMapValidMapWithAttachmentsDataExcludedNonWinningRev() throws Exception {

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
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        InputStream attachmentInputStream = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        InputStream expectedInputStream = new ByteArrayInputStream(unencodedAttachment);
        Assert.assertTrue(TestUtils.streamsEqual(expectedInputStream, attachmentInputStream));

    }

    @Test
    public void buildRevisionFromMapValidMapWithTextAttachmentsDataExcluded() throws Exception {

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
        Assert.assertEquals(body,revision.getBody().asMap());
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
        remoteBonsai.put("length",encodedAttachmentString.length());
        remoteBonsai.put("digest","thisisahasiswear");
        remoteBonsai.put("revpos",1);
        remoteBonsai.put("content_type",contentType);
        remoteBonsai.put("stub",stub);
        if(!stub) {
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