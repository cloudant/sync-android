/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

import static com.cloudant.sync.internal.mazha.matcher.IsNotEmpty.notEmpty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;

import com.cloudant.sync.internal.common.CouchConstants;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.http.Http;
import com.cloudant.http.HttpConnection;
import com.cloudant.sync.internal.mazha.json.JSONHelper;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Assert;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class ClientTestUtils {

    private static JSONHelper jsonHelper = new JSONHelper();

    public static Response createHelloWorldDoc(CouchClient client) {
        Map<String, Object> doc = getHelloWorldObject();
        Response res = client.create(doc);
        assertDocumentCreatedCorrectly(res);
        return res;
    }

    private static void assertDocumentCreatedCorrectly(Response res) {
        Assert.assertThat("Document id", res.getId(), is(notEmpty()));
        Assert.assertThat("Document revision", res.getRev(), is(notEmpty()));
        Assert.assertThat("Revision", res.getRev(), startsWith("1-"));
    }

    protected static Map<String, Object> getHelloWorldObject() {
        Map<String, Object> doc = new HashMap<String, Object>();
        doc.put("hello", "world");
        return doc;
    }

    /**
     *
     * Convenience method to fetch and update document object with the given revision and revision history.
     * The returned document object has the following property:
     *
     * document._rev = revision
     * document._revisions = revisions
     * document.name = name
     *
     */
    private static Map<String, Object> updateDocumentWithRevisionHistory(CouchClient client,
                                                                  String documentId,
                                                                  String revision,
                                                                  Map<String, Object> revisions,
                                                                  String name) {
        Map<String, Object> doc = client.getDocument(documentId);
        doc.put(CouchConstants._rev, revision);
        doc.put(CouchConstants._revisions, revisions);
        doc.put("name", name);
        return doc;
    }

    /**
     * Create a conflicts to specified document using bulkCreateDocs api. The document is specified by <code>Response</code>,
     * which usually is the response back from <code>ClientTestUtils.createHelloWorldDoc</code>
     *
     * And, the document tree looks like this:
     *
     * 1 -> 2 -> 3
     *  \-> 2*
     *  \-> 2**
     *
     * return all open revisions.
     */
    public static String[] createDocumentWithConflicts(CouchClient client, Response res) {

        String rev1 = res.getRev();
        String rev2 = CouchUtils.generateNextRevisionId(rev1);
        String rev3 = CouchUtils.generateNextRevisionId(rev2);
        String rev2Star = CouchUtils.generateNextRevisionId(rev1);
        String rev2StarStar = CouchUtils.generateNextRevisionId(rev1);

        Map<String, Object> revs1 = getRevisionHistory(rev3, rev2, rev1);
        Map<String, Object> revs2 = getRevisionHistory(rev2Star, rev1);
        Map<String, Object> revs3 = getRevisionHistory(rev2StarStar, rev1);

        Map<String, Object> docToUpdate1 = updateDocumentWithRevisionHistory(client, res.getId(), rev3, revs1, "Tom");
        Map<String, Object> docToUpdate2 = updateDocumentWithRevisionHistory(client, res.getId(), rev2Star, revs2,
                "Jerry");
        Map<String, Object> docToUpdate3 = updateDocumentWithRevisionHistory(client, res.getId(), rev2StarStar, revs3, "Alex");

        List<Response> responses = client.bulkCreateDocs(docToUpdate1, docToUpdate2, docToUpdate3);

        Assert.assertThat("Responses list", responses.size(), is(equalTo(0)));

        Map<String, Object> updatedDoc = client.getDocument(res.getId());
        Assert.assertThat("Updated document", updatedDoc.keySet(), hasItem(CouchConstants._rev));
        Assert.assertThat("Current revision", (String)updatedDoc.get(CouchConstants._rev), startsWith("3-"));
        Assert.assertThat("Updated document", updatedDoc.keySet(), hasItem("name"));
        Assert.assertThat("Updated document", (String)updatedDoc.get("name"), is(equalTo("Tom")));

        return new String[]{rev3, rev2Star, rev2StarStar};
    }

    /**
     * Create a conflicts with separate roots to a specified document using bulkCreateDocs api. The document is
     * specified by <code>Response</code> which usually is the response back from <code>ClientTestUtils.createHelloWorldDoc</code>
     *
     * And, the document tree (or forest) looks like this:
     *
     * 1 -> 2 -> 3
     *
     * 1* ->2*
     *
     * return all open revisions.
     */

    public static String[] createDocumentForest(CouchClient client, Response res) {

        String rev1 = res.getRev();
        String rev2 = CouchUtils.generateNextRevisionId(rev1);
        String rev3 = CouchUtils.generateNextRevisionId(rev2);

        Map<String, Object> revs1 = getRevisionHistory(rev3, rev2, rev1);
        Map<String, Object> docToUpdate1 = updateDocumentWithRevisionHistory(client, res.getId(), rev3, revs1, "Tom");

        String rev1Star = CouchUtils.getFirstRevisionId();
        String rev2Star = CouchUtils.generateNextRevisionId(rev1Star);
        Map<String, Object> revs2 = getRevisionHistory(rev2Star, rev1Star);

        Map<String, Object> docToUpdate2 = updateDocumentWithRevisionHistory(client, res.getId(), rev2Star, revs2, "Jerry");
        List<Response> responses = client.bulkCreateDocs(docToUpdate1, docToUpdate2);

        Assert.assertThat("Responses list", responses.size(), is(equalTo(0)));
        return new String[]{rev3, rev2Star};
    }

    /**
     * Given a list of revision is the reverse order, and return a revision history like of <code>Map</code> object,
     * For example, the result is a JSON Object with following structure:
     *
     * {
     *     "start" : 2,
     *     "ids" : [
     *         "dc16dcd3a3faa8a6b5cdc21b2e16d6a4",
     *         "15f65339921e497348be384867bb940f"
     *     ]
     * }
     *
     * For given input
     *
     * [
     *     "2-dc16dcd3a3faa8a6b5cdc21b2e16d6a4",
     *     "1-15f65339921e497348be384867bb940f"
     * ]
     */
    public static Map<String, Object> getRevisionHistory(String... revisions) {
        Map<String, Object> revHistory = new HashMap<String, Object>();
        revHistory.put(CouchConstants.start, CouchUtils.generationFromRevId(revisions[0]));
        revHistory.put(CouchConstants.ids, getRevisionHashList(revisions));
        return revHistory;
    }

    private static List<String> getRevisionHashList(String[] revisions) {
        List<String> revisionHashes = new ArrayList<String>();
        for (String rev : revisions) {
            revisionHashes.add(CouchUtils.getRevisionIdSuffix(rev));
        }
        return revisionHashes;
    }

    public static void deleteQuietly(CouchClient client) {
        try {
            client.deleteDb();
        } catch (Exception e) {}
    }


    public static int executeHttpPostRequest(URI uri, String payload) {
        HttpConnection connection = Http.connect("POST", uri, "application/json");
        try {
            connection.setRequestBody(payload);
            connection.execute();
        } catch (Exception e) {
            ; // ignore exception
        }
        try {
            return connection.getConnection().getResponseCode();
        } catch (Exception e) {
            System.out.println("*** got ex "+e);
            return 0;
        }
    }

    public static List<String> getRemoteRevisionIDs(URI uri) throws Exception{
        HttpConnection connection = Http.GET(uri);
        InputStream in = connection.execute().responseAsInputStream();

        JSONObject jsonObject = new JSONObject(new JSONTokener(IOUtils.toString(in)));
        JSONArray revsInfo = jsonObject.getJSONArray("_revs_info");

        List<String> revisions = new ArrayList<String>(revsInfo.length());

        for(int i=0; i<revsInfo.length(); i++){
            revisions.add(revsInfo.getJSONObject(i).getString("rev"));
        }

        return revisions;
    }


}
