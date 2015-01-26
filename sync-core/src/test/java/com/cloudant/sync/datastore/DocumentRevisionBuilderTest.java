package com.cloudant.sync.datastore;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DocumentRevisionBuilderTest  {

    Map<String,Object> documentRev;
    Map<String, String>body;
    URI documentURI;

    @Before
    public void setUp() throws Exception {
        // this URI is needed by buildRevisionFromMap in case it needs to download the attachments
        // given in the _attachments part of the body
        // since we don't care about attachments here, pass a dummy value
        documentURI = new URI("http://not-a-real-host");

        documentRev  = new HashMap<String,Object>();
        documentRev.put("_id","someIdHere");
        documentRev.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        documentRev.put("aKey","aValue");
        documentRev.put("hello","world");

        body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");
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
}
