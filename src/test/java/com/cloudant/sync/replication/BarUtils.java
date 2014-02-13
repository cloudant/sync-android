/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.replication;

import com.cloudant.common.CouchConstants;
import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.TypedDatastore;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;

public class BarUtils {

    private static TypedDatastore<Bar> getBarDatastore(Datastore db) {
        return new TypedDatastore<Bar>(Bar.class, (DatastoreExtended)db);
    }

    public static Bar createBar(CouchClientWrapper db, String id, String name, int age) {
        Bar bar = new Bar();
        bar.setId(id);
        bar.setName(name);
        bar.setAge(age);

        Response res = db.create(bar);
        Bar savedBar = db.get(Bar.class, res.getId());
        Assert.assertNotNull(savedBar);
        Assert.assertEquals(id, res.getId());
        Assert.assertThat(savedBar.getRevision(), startsWith("1-"));

        return savedBar;
    }

    public static Bar createBar(CouchClientWrapper db, String name, int age) {
        Bar bar = new Bar();
        bar.setName(name);
        bar.setAge(age);

        Response res = db.create(bar);
        Bar savedBar = db.get(Bar.class, res.getId());
        Assert.assertNotNull(savedBar);
        Assert.assertThat(savedBar.getRevision(), startsWith("1-"));

        return savedBar;
    }

    public static Bar createBar(Datastore db, String id, String name, int age) {
        Bar bar = new Bar();
        bar.setId(id);
        bar.setName(name);
        bar.setAge(age);

        TypedDatastore<Bar> datastore = getBarDatastore((DatastoreExtended) db);
        Bar barCreated = datastore.createDocument(bar);

        Assert.assertNotNull(barCreated);
        Assert.assertThat(barCreated.getRevision(), startsWith("1-"));
        return barCreated;
    }

    public static Bar createBar(Datastore db, String name, int age) {
        Bar bar = new Bar();
        bar.setName(name);
        bar.setAge(age);

        TypedDatastore<Bar> datastore = getBarDatastore(db);
        Bar barCreated = datastore.createDocument(bar);

        Assert.assertNotNull(barCreated);
        Assert.assertThat(barCreated.getRevision(), startsWith("1-"));
        return barCreated;
    }

    public static Bar updateBar(CouchClientWrapper db, String id, String name, int age) {
        Bar bar = db.get(Bar.class, id);
        bar.setName(name);
        bar.setAge(age);
        int oldGeneration = CouchUtils.generationFromRevId(bar.getRevision());

        Response res = db.update(bar.getId(), bar);
        Bar updatedBar = db.get(Bar.class, res.getId());
        Assert.assertNotNull(updatedBar);
        Assert.assertThat(updatedBar.getRevision(), startsWith((oldGeneration + 1) + "-"));
        return updatedBar;
    }

    public static Bar updateBar(Datastore db, String id, String name, int age) throws ConflictException {
        TypedDatastore<Bar> datastore = getBarDatastore(db);
        Bar bar = datastore.getDocument(id);
        bar.setName(name);
        bar.setAge(age);
        int oldGeneration = CouchUtils.generationFromRevId(bar.getRevision());

        Bar bar2 = datastore.updateDocument(bar);
        Assert.assertNotNull(bar2);
        Assert.assertThat(bar2.getRevision(), startsWith((oldGeneration + 1) + "-"));
        return bar2;
    }

    public static Response deleteBar(CouchClientWrapper db, String id) {
        Bar bar = db.get(Bar.class, id);
        int oldGeneration = CouchUtils.generationFromRevId(bar.getRevision());

        Response res = db.delete(bar.getId(), bar.getRevision());
        Assert.assertNotNull(res);
        Assert.assertThat(res.getRev(), startsWith((oldGeneration + 1) + "-"));
        return res;
    }

    public static void deleteBar(Datastore db, String id) throws ConflictException {
        TypedDatastore<Bar> datastore = getBarDatastore(db);
        Bar bar = datastore.getDocument(id);
        datastore.deleteDocument(bar);
    }

    /**
     * Create a conflicts to specified document using bulk api. The document is specified by <code>Response</code>,
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
    public static String[] createThreeLeafs(CouchClientWrapper db, String id) {
        Bar bar = db.get(Bar.class, id);
        int oldGeneration = CouchUtils.generationFromRevId(bar.getRevision());

        String rev1 = bar.getRevision();
        String rev2 = com.cloudant.common.CouchUtils.generateNextRevisionId(rev1);
        String rev3 = com.cloudant.common.CouchUtils.generateNextRevisionId(rev2);
        String rev2Star = com.cloudant.common.CouchUtils.generateNextRevisionId(rev1);
        String rev2StarStar = com.cloudant.common.CouchUtils.generateNextRevisionId(rev1);

        Map<String, Object> revs1 = getRevisionHistory(rev3, rev2, rev1);
        Map<String, Object> revs2 = getRevisionHistory(rev2Star, rev1);
        Map<String, Object> revs3 = getRevisionHistory(rev2StarStar, rev1);

        Map<String, Object> docToUpdate1 = updateDocumentWithRevisionHistory(db.getCouchClient(), bar.getId(), rev3, revs1, "Tom");
        Map<String, Object> docToUpdate2 = updateDocumentWithRevisionHistory(db.getCouchClient(), bar.getId(), rev2Star, revs2, "Jerry");
        Map<String, Object> docToUpdate3 = updateDocumentWithRevisionHistory(db.getCouchClient(), bar.getId(), rev2StarStar, revs3, "Alex");

        List<Response> responses = db.getCouchClient().bulk(docToUpdate1, docToUpdate2, docToUpdate3);

        Assert.assertThat("Responses list", responses.size(), is(equalTo(0)));

        Map<String, Object> updatedDoc = db.getCouchClient().getDocument(bar.getId());
        Assert.assertThat("Updated document", updatedDoc.keySet(), hasItem(CouchConstants._rev));
        Assert.assertThat("Current revision", (String)updatedDoc.get(CouchConstants._rev), startsWith("3-"));
        Assert.assertThat("Updated document", updatedDoc.keySet(), hasItem("name"));
        Assert.assertThat("Updated document", (String)updatedDoc.get("name"), is(equalTo("Tom")));

        return new String[]{rev3, rev2Star, rev2StarStar};
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
        revHistory.put(CouchConstants.start, com.cloudant.common.CouchUtils.generationFromRevId(revisions[0]));
        revHistory.put(CouchConstants.ids, getRevisionHashList(revisions));
        return revHistory;
    }

    private static List<String> getRevisionHashList(String[] revisions) {
        List<String> revisionHashes = new ArrayList<String>();
        for (String rev : revisions) {
            revisionHashes.add(com.cloudant.common.CouchUtils.getRevisionIdSuffix(rev));
        }
        return revisionHashes;
    }



}
