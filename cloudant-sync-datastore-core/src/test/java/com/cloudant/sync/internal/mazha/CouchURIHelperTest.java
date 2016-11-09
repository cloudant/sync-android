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


package com.cloudant.sync.internal.mazha;

import com.cloudant.sync.internal.mazha.CouchURIHelper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@RunWith(Parameterized.class)
public class CouchURIHelperTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {""}, {"/api/couch/account_2128459498a75498"}
        });
    }

    String protocol = "http";
    String hostname = "127.0.0.1";
    int port = 5984;
    @Parameterized.Parameter
    public String path;
    String uriBase;

    @Before
    public void setup() {
        uriBase = protocol+"://"+hostname+":"+port+path;
    }

    // make a helper for given db name
    private CouchURIHelper helper(String dbName) throws URISyntaxException {
        return new CouchURIHelper(new URI(protocol, null, hostname, port, dbName, null, null));
    }

    @Test
    public void _localDocumentURI() throws Exception {
        final String expected = uriBase + "/db_name/_local/mylocaldoc";

        CouchURIHelper helper = helper(path+"/db_name");
        URI localDoc = helper.documentUri("_local/mylocaldoc");

        Assert.assertEquals(expected,localDoc.toString());
    }

    @Test
    public void buildDbUri() throws Exception {
        URI expected = new URI(uriBase + "/db_name");
        URI actual = helper(path+"/db_name").getRootUri();
        Assert.assertEquals(expected, actual);
    }

    // this test shows that non-ascii characters will be represented correctly
    // in the url but that we don't escape characters like /
    @Test
    public void buildEscapedDbUri() throws Exception {
        URI expected = new URI(uriBase + "/SDF@%23%25$%23)DFGKLDfdffdg%C3%A9");
        URI actual = helper(path+"/SDF@#%$#)DFGKLDfdffdg\u00E9").getRootUri();
        Assert.assertEquals(expected.toASCIIString(), actual.toASCIIString());
    }

    @Test
    public void buildChangesUri_options_optionsEncoded() throws Exception {
        URI expected = new URI(uriBase + "/test/_changes?limit=100&since=%22%5B%5D%22");

        Map<String, Object> options = new HashMap<String, Object>();
        options.put("since", "\"[]\"");
        options.put("limit", 100);
        URI actual = helper(path+"/test").changesUri(options);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildChangesUri_woOptions() throws Exception {
        URI expected = new URI(uriBase + "/test/_changes");
        URI actual = helper(path+"/test").changesUri(new HashMap<String, Object>());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildBulkDocsUri() throws Exception {
        URI expected = new URI(uriBase + "/test/_bulk_docs");
        URI actual = helper(path+"/test").bulkDocsUri();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void revsDiffUri() throws Exception {
        URI expected = new URI(uriBase + "/test/_revs_diff");
        URI actual = helper(path+"/test").revsDiffUri();
        Assert.assertEquals(expected, actual);
    }

    // get a document with a db 'mounted' at /
    @Test
    public void buildDocumentUri_emptyDb() throws Exception {
        URI expected = new URI(uriBase + "/documentId");
        URI actual = helper(path).documentUri("documentId");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_woOptions() throws Exception {
        URI expected = new URI(uriBase + "/test/documentId");
        URI actual = helper(path+"/test").documentUri("documentId");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_slashInDocumentId() throws Exception {
        URI expected = new URI(uriBase + "/test/path1%2Fpath2");
        URI actual = helper(path+"/test").documentUri("path1/path2");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_specialCharsInDocumentId() throws Exception {
        URI expected = new URI(uriBase + "/test/SDF@%23%25$%23)DFGKLDfdffdg%C3%A9");
        URI actual = helper(path+"/test").documentUri("SDF@#%$#)DFGKLDfdffdg\u00E9");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_colonInDocumentId() throws Exception {
        URI expected = new URI(uriBase + "/test/:this:has:colons:");
        URI actual = helper(path+"/test").documentUri(":this:has:colons:");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_options_optionsEncoded() throws Exception {
        URI expected = new URI(uriBase + "/test/path1%2Fpath2?detail=true&revs=%5B1-2%5D");

        Map<String, Object> options = new TreeMap<String, Object>();
        options.put("revs", "[1-2]");
        options.put("detail", true);
        URI actual = helper(path+"/test").documentUri("path1/path2", options);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_options_encodeSeparators() throws Exception {
        URI expected = new URI(uriBase + "/test/path1%2Fpath2?d%26etail%3D=%26%3D%3Dds%26&revs=%5B1-2%5D");

        TreeMap<String, Object> options = new TreeMap<String, Object>();
        options.put("revs", "[1-2]");
        options.put("d&etail=", "&==ds&");
        URI actual = helper(path+"/test").documentUri("path1/path2", options);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void buildDocumentUri_options_hasPlus() throws Exception {
        URI expected = new URI(uriBase + "/test/path1%2Fpath2?q=class:mammal%2Bwith%2Bplusses");

        TreeMap<String, Object> options = new TreeMap<String, Object>();
        options.put("q", "class:mammal+with+plusses");
        URI actual = helper(path+"/test").documentUri("path1/path2", options);
        Assert.assertEquals(expected, actual);
    }


    // this test shows that non-ascii characters will be represented correctly
    // in the url but that we don't escape characters like / in the root url, but that they are
    // correctly escaped in the document part of the url
    @Test
    public void buildVeryEscapedUri() throws Exception {
        URI expected = new URI(uriBase + "/SDF@%23%25$%23)KLDfdffdg%C3%A9/%2FSF@%23%25$%23)DFGKLDfdffdg%C3%A9%2Fpath2?detail=/SDF@%23%25$%23)%C3%A9&revs=%5B1-2%5D");

        Map<String, Object> options = new TreeMap<String, Object>();
        options.put("revs", "[1-2]");
        options.put("detail", "/SDF@#%$#)\u00E9");
        URI actual = helper(path+"/SDF@#%$#)KLDfdffdg\u00E9").documentUri("/SF@#%$#)" +
                "DFGKLDfdffdg\u00E9/path2", options);

        Assert.assertEquals(expected.toASCIIString(), actual.toASCIIString());
    }


    @Test
    public void encodePathComponent_slashShouldBeEncoded() throws Exception {
        String in = "/path1/path2";
        Assert.assertEquals("%2Fpath1%2Fpath2", helper(path+"/test").encodeId(in));
    }

    @Test
    public void encodeQueryParameter_noLeadingQuestionMark() throws Exception {
        String in = "a";
        Assert.assertTrue(helper(path+"/test").encodeId(in).charAt(0) != '?');
    }

}
