
package com.cloudant.mazha;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class CouchURIHelperTest {

    CouchURIHelper helper;
    String protocol = "http";
    String hostname = "127.0.0.1";
    int port = 5984;
    String db = "test";
    String uriBase = "http://127.0.0.1:5984/";

    @Before
    public void setup() {
        helper = new CouchURIHelper(protocol, hostname, port);
    }

    URI create(String s) throws URISyntaxException {
        return new URI(s);
    }

    @Test
    public void buildAllDbUri() throws Exception{
        URI expected = this.create(uriBase + "_all_dbs");
        URI actual = helper.allDbsUri();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDbUri() throws Exception {
        URI expected = this.create(uriBase + "db_name");
        URI actual = helper.dbUri("db_name");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildEscapedDbUri() throws Exception {
        URI expected = this.create(uriBase + "%2FSDF@%23%25$%23)DFGKLDfdffdg%C3%A9");
        URI actual = helper.dbUri("/SDF@#%$#)DFGKLDfdffdg\u00E9");
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = NullPointerException.class)
    public void buildDbUri_null_exception() throws Exception {
        helper.dbUri(null);
    }

    @Test
    public void buildChangesUri_options_optionsEncoded() throws Exception {
        URI expected = this.create(uriBase + "test/_changes?limit=100&since=%22[]");

        Map<String, Object> options = new HashMap<String, Object>();
        options.put("since", "\"[]");
        options.put("limit", 100);
        URI actual = helper.changesUri(db, options);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildChangesUri_woOptions() throws Exception {
        URI expected = this.create(uriBase + "test/_changes");
        URI actual = helper.changesUri(db, new HashMap<String, Object>());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildBulkDocsUri() throws Exception {
        URI expected = this.create(uriBase + "test/_bulk_docs");
        URI actual = helper.bulkDocsUri(db);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void revsDiffUri() throws Exception {
        URI expected = this.create(uriBase + "test/_revs_diff");
        URI actual = helper.revsDiffUri(db);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_woOptions() throws Exception {
        URI expected = this.create(uriBase + "test/documentId");
        URI actual = helper.documentUri(db, "documentId");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_slashInDocumentId() throws Exception {
        URI expected = this.create(uriBase + "test/path1%2Fpath2");
        URI actual = helper.documentUri(db, "path1/path2");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_specialCharsInDocumentId() throws Exception {
        URI expected = this.create(uriBase + "test/SDF@%23%25$%23)DFGKLDfdffdg%C3%A9");
        URI actual = helper.documentUri(db, "SDF@#%$#)DFGKLDfdffdg\u00E9");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_options_optionsEncoded() throws Exception {
        URI expected = this.create(uriBase + "test/path1%2Fpath2?detail=true&revs=[1-2]");

        Map<String, Object> options = new HashMap<String, Object>();
        options.put("revs", "[1-2]");
        options.put("detail", true);
        URI actual = helper.documentUri(db, "path1/path2", options);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_options_encodeSeparators() throws Exception {
        URI expected = this.create(uriBase + "test/path1%2Fpath2?d%26etail%3D=%26%3D%3Dds%26&revs=[1-2]");

        Map<String, Object> options = new HashMap<String, Object>();
        options.put("revs", "[1-2]");
        options.put("d&etail=", "&==ds&");
        URI actual = helper.documentUri(db, "path1/path2", options);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildVeryEscapedUri() throws Exception {
        URI expected = this.create(uriBase + "%2FSDF@%23%25$%23)KLDfdffdg%C3%A9/%2FSF@%23%25$%23)DFGKLDfdffdg%C3%A9%2Fpath2?detail=/SDF@%23%25$%23)%C3%A9&revs=[1-2]");

        Map<String, Object> options = new HashMap<String, Object>();
        options.put("revs", "[1-2]");
        options.put("detail", "/SDF@#%$#)\u00E9");
        URI actual = helper.documentUri("/SDF@#%$#)KLDfdffdg\u00E9", "/SF@#%$#)DFGKLDfdffdg\u00E9/path2", options);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void encodePathComponent_slashShouldBeEncoded() throws Exception {
        String in = "/path1/path2";
        Assert.assertEquals("%2Fpath1%2Fpath2", helper.encodePathComponent(in));
    }

    @Test
    public void encodeQueryParameter_noLeadingQuestionMark() throws Exception {
        String in = "a";
        Assert.assertTrue(helper.encodePathComponent(in).charAt(0) != '?');
    }

}
