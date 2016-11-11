/**
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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.startsWith;

import com.cloudant.common.CollectionFactory;
import com.cloudant.sync.internal.common.CouchConstants;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.sync.internal.util.Misc;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Category(RequireRunningCouchDB.class)
public class CouchClientBasicTest extends CouchClientTestBase {

    // Some of these tests don't use 'client' from the base class and should probably be moved
    // to another class

    @Test
    public void getJsonHelper_mustNotBeNull() {
        Assert.assertNotNull(client.jsonHelper);
    }

    @Test
    public void createDb_validDbName_dbMustBeCreated() {
        String dbName = "mazha_test_createdb"+System.currentTimeMillis();
        CouchConfig config = getCouchConfig(dbName);
        CouchClient customClient = new CouchClient(config.getRootUri(), config.getRequestInterceptors(), config.getResponseInterceptors());
        ClientTestUtils.deleteQuietly(customClient);

        customClient.createDb();
        Assert.assertTrue("DB must exist.", isDbExist(dbName));

        customClient.deleteDb();
        Assert.assertFalse("DB must not exist.", isDbExist(dbName));
    }

    @Test(expected = CouchException.class)
    public void createDb_invalidDBNmae_exception() {
        // Couch does not like capital character in db name
        String dbName = "mazha_test_INVALID_DB_NAME";
        CouchConfig config = getCouchConfig(dbName);
        CouchClient customClient = new CouchClient(config.getRootUri(), config.getRequestInterceptors(), config.getResponseInterceptors());
        customClient.createDb();
    }


    @Test(expected = CouchException.class)
    public void createDb_dbExistAlready_exception() throws IOException {
        Assert.assertTrue("DB must exist.", isDbExist(testDb));
        client.createDb();
    }

    @Test
    public void deleteDb_dbMustBeDeleted() {
        String dbName = "mazha_test_deletedb"+System.currentTimeMillis();
        CouchConfig config = getCouchConfig(dbName);
        CouchClient customClient = new CouchClient(config.getRootUri(), config.getRequestInterceptors(), config.getResponseInterceptors());
        ClientTestUtils.deleteQuietly(client);

        customClient.createDb();
        Assert.assertTrue("DB must exist.", isDbExist(dbName));

        customClient.deleteDb();
        Assert.assertFalse("DB must not exist.", isDbExist(dbName));
    }

    @Test(expected = NoResourceException.class)
    public void deleteDb_dbNotExist_exception() {
        String dbName = "mazha_test_deletedb_not_exist";
        CouchConfig config = getCouchConfig(dbName);
        CouchClient customClient = new CouchClient(config.getRootUri(), config.getRequestInterceptors(), config.getResponseInterceptors());
        Misc.checkArgument(!isDbExist(dbName), String.format("%s must not exist", dbName));
        customClient.deleteDb();
    }

    @Test
    public void getDbInfo_dbInfoMustSuccessfullyReturned() {
        CouchDbInfo dbInfo = client.getDbInfo();
        Assert.assertEquals(testDb, dbInfo.getDbName());
    }

    @Test(expected = CouchException.class)
    public void getDbInfo_invalidDbName_exception() {
        String dbName = "mazha_test_A";
        CouchConfig config = getCouchConfig(dbName);
        CouchClient customClient = new CouchClient(config.getRootUri(), config.getRequestInterceptors(), config.getResponseInterceptors());
        customClient.getDbInfo();
    }

    @Test(expected = NoResourceException.class)
    public void getDbInfo_dbNotExist_exception() {
        String dbName = "mazha_test_getdbinfo_dbnotexist";
        CouchConfig config = getCouchConfig(dbName);
        CouchClient customClient = new CouchClient(config.getRootUri(), config.getRequestInterceptors(), config.getResponseInterceptors());
        Misc.checkArgument(!isDbExist(dbName), String.format("%s must not exist", dbName));
        customClient.getDbInfo();
    }

    @Test
    public void create_documentWithoutId_success() {
        ClientTestUtils.createHelloWorldDoc(client);
    }

    @Test
    public void create_idInChinese_success() {
        String id = "\u6768\u6728\u91d1";
        Map<String, Object> doc = new HashMap<String, Object>();
        doc.put("_id", id);
        doc.put("hello", "world");
        Response res = client.create(doc);
        Assert.assertNotNull(res);
        Assert.assertTrue(id.equals(res.getId()));
        Assert.assertThat(res.getRev(), startsWith("1-"));

        Map<String, Object> doc2 = client.getDocument(id);
        Assert.assertEquals(id, doc2.get("_id"));
        Assert.assertEquals(res.getRev(), doc2.get("_rev"));
        Assert.assertEquals("world", doc2.get("hello"));
    }

    @Test
    public void create_documentWithIdNotExist_success() {
        Map<String, Object> doc = ClientTestUtils.getHelloWorldObject();
        doc.put(CouchConstants._id, "SomeUniqueId");

        Response res = client.create(doc);
        Assert.assertTrue(!Misc.isStringNullOrEmpty(res.getId()));
        Assert.assertTrue(!Misc.isStringNullOrEmpty(res.getRev()));
    }

    @Test(expected = DocumentConflictException.class)
    public void create_documentWithIdExist_conflict() {
        Map<String, Object> doc = ClientTestUtils.getHelloWorldObject();
        doc.put(CouchConstants._id, "SomeUniqueId");

        {
            Response res = client.create(doc);
            Assert.assertTrue(!Misc.isStringNullOrEmpty(res.getId()));
            Assert.assertTrue(!Misc.isStringNullOrEmpty(res.getRev()));
        }

        {
            client.create(doc);
        }
    }

    @Test
    public void getDocumentInputStream_id_success() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        InputStream is = client.getDocumentStream(res.getId(), res.getRev());
        try {
            Map<String, Object> doc = client.jsonHelper.fromJson(new InputStreamReader(is));
            assertHelloWorldMapObject(res, doc);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Test
    public void getDocument_id_success() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Map<String, Object> doc = client.getDocument(res.getId());
        assertHelloWorldMapObject(res, doc);
    }

    private void assertHelloWorldMapObject(Response res, Map<String, Object> doc) {
        Assert.assertTrue(doc.containsKey("hello"));
        Assert.assertEquals("world", doc.get("hello"));
        Assert.assertTrue(doc.containsKey(CouchConstants._id));
        Assert.assertEquals(res.getId(), doc.get(CouchConstants._id));
        Assert.assertTrue(doc.containsKey(CouchConstants._rev));
        Assert.assertEquals(res.getRev(), doc.get(CouchConstants._rev));
    }

    @Test(expected = NoResourceException.class)
    public void getDocumentInputStream_idNotExist_exception() {
        client.getDocumentStream("id_not_exist", "1-no_such_rev");
    }

    @Test(expected = NoResourceException.class)
    public void getDocument_idNotExist_exception() {
        client.getDocument("id_not_exist");
    }

    @Test
    public void getDocument_idRev_success() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Map<String, Object> doc = client.getDocument(res.getId(), res.getRev());
        assertHelloWorldMapObject(res, doc);
    }

    @Test
    public void getDocument_idForFoo_success() {
        Foo foo = new Foo("Tom");
        Response res = client.create(foo);
        Foo fooRetrieved = client.getDocument(res.getId(), Foo.class);
        Assert.assertNotNull(fooRetrieved);
    }

    @Test
    public void getDocument_idRevForFoo_success() {
        Response res = client.create(new Foo("Tom"));
        Assert.assertThat(res.getRev(), startsWith("1-"));

        Foo foo = client.getDocument(res.getId(), Foo.class);
        foo.setName("Jerry");
        Response res2 = client.create(foo);
        Assert.assertThat(res2.getRev(), startsWith("2-"));

        {
            Foo fooVersion2 = client.getDocument(res2.getId(), Foo.class);
            Assert.assertNotNull(fooVersion2);
            Assert.assertEquals(res2.getId(), fooVersion2.getId());
            Assert.assertEquals(res2.getRev(), fooVersion2.getRevision());
        }

        {
            Foo fooVersion1 = client.getDocument(res.getId(), res.getRev(), Foo.class);
            Assert.assertNotNull(fooVersion1);
            Assert.assertThat(fooVersion1.getRevision(), startsWith("1-"));
        }
    }

    @Test
    public void getDocumentInputStream_idRev_success() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        InputStream is = client.getDocumentStream(res.getId(), res.getRev());
        try {
            Map<String, Object> doc = client.jsonHelper.fromJson(new InputStreamReader(is));
            assertHelloWorldMapObject(res, doc);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Test(expected = NoResourceException.class)
    public void getDocumentInputStream_idRevNotExist_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        client.getDocumentStream(res.getId(), "1-revnotexist");
    }

    @Test(expected = NoResourceException.class)
    public void getDocument_idRevNotExist_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        client.getDocument(res.getId(), "1-revnotexist", Map.class);
    }

    @Test
    public void contains_success_mustReturnTrue() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Assert.assertTrue(client.contains(res.getId()));
    }

    @Test
    public void contains_idNotExist_mustReturnFalse() {
        Assert.assertFalse(client.contains("id_not_exist"));
    }

    @Test
    public void update_success() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Map<String, Object> doc = client.getDocument(res.getId());
        doc.put("Here", "Is Cloudant");
        Response res2 = client.update(res.getId(), doc);
        Assert.assertEquals(res.getId(), res2.getId());
        Assert.assertTrue(res2.getRev().startsWith("2-"));

        Map<String, Object> updatedDoc = client.getDocument(res.getId());
        this.assertHelloWorldMapObject(res2, updatedDoc);
        Assert.assertTrue(updatedDoc.containsKey("Here"));
        Assert.assertEquals("Is Cloudant", updatedDoc.get("Here"));
    }

    @Test
    public void update_foo_success() {
        Response res = client.create(new Foo("Tom"));
        Foo foo1 = client.getDocument(res.getId(), Foo.class);
        foo1.setName("Jerry");

        Response updateRes = client.update(res.getId(), foo1);
        Assert.assertEquals(res.getId(), updateRes.getId());
        Assert.assertThat(updateRes.getRev(), startsWith("2-"));

        Foo foo2 = client.getDocument(res.getId(), Foo.class);
        Assert.assertEquals("Jerry", foo2.getName());
    }

    @Test(expected = NoResourceException.class)
    public void update_idNotExist_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Map<String, Object> doc = client.getDocument(res.getId());
        doc.put(CouchConstants._id, "some_id_not_exist");
        client.update("some_id_not_exist", doc);
    }

    @Test
    public void update_idDoesNotMatchDocument_idInDocumentIsIgnored() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Map<String, Object> doc = client.getDocument(res.getId());
        doc.put(CouchConstants._id, "some_id_not_exist");
        Response res2 = client.update(res.getId(), doc);
        Map<String, Object> updatedDoc = client.getDocument(res2.getId());
        Assert.assertThat((String) updatedDoc.get(CouchConstants._rev), startsWith("2-"));
    }

    @Test(expected = DocumentConflictException.class)
    public void update_revNotMatch_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Map<String, Object> doc = client.getDocument(res.getId());
        doc.put(CouchConstants._rev, "1-some_invalid_rev");
        doc.put("Here", "Is Cloudant");

        client.update(res.getId(), doc);
    }

    @Test
    public void delete_success() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Response deleteRes = client.delete(res.getId(), res.getRev());
        Assert.assertEquals(res.getId(), deleteRes.getId());
        Assert.assertTrue(deleteRes.getRev().startsWith("2-"));
    }

    @Test(expected = NoResourceException.class)
    public void delete_idNotExist_exception() {
        client.delete("some_id_not_exist", "1-some_rev_not_exist");
    }

    @Test(expected = DocumentConflictException.class)
    public void delete_revNotExist_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        client.delete(res.getId(), "1-some_rev_not_exist");
    }

    @Test
    public void getDocumentOldRev_docDeleted_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        client.delete(res.getId(), res.getRev());
        Map res3 = client.getDocument(res.getId(), res.getRev(), Map.class);
        Assert.assertEquals(res3.get(CouchConstants._rev), res.getRev());
    }

    @Test
    public void getDocumentLatestRev_docDeleted_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Response res2 = client.delete(res.getId(), res.getRev());
        Map res3 = client.getDocument(res2.getId(), res2.getRev(), Map.class);
        Assert.assertEquals(Boolean.TRUE, res3.get(CouchConstants._deleted));
    }

    @Test
    @Ignore
    // Currently this test fails on Cloudant. The behaviour is likely to change in future, at which
    // point the test can be re-instated.
    public void revsDiff_emptyInput_returnNothing() {
        Map<String, CouchClient.MissingRevisions> diffs = client.revsDiff(new HashMap<String, Set<String>>());
        Assert.assertEquals(0, diffs.size());
    }

    @Test
    public void revsDiff_oneDocWith10000Revisions_returnEverything() {
        Set<String> revisions = new HashSet<String>();
        for(int i = 0 ; i < 10000 ; i ++) {
            revisions.add(String.valueOf(i + "-a" ));
        }
        Map<String, Set<String>> revs = CollectionFactory.MAP.of("A", revisions);
        Map<String, CouchClient.MissingRevisions> diffs = client.revsDiff(revs);

        Assert.assertEquals(1, diffs.size());
        Assert.assertEquals(10000, diffs.get("A").missing.size());
        Assert.assertThat(diffs.get("A").missing, hasItems("0-a", "9999-a"));
    }

    @Test
    public void revsDiff_twoDocs_returnTwoDocs() {
        Set<String> revs1 = CollectionFactory.SET.of("1-a", "2-b");
        Set<String> revs2 = CollectionFactory.SET.of("1-c", "2-d");
        Map<String, Set<String>> revs = CollectionFactory.MAP.of("A", revs1, "B", revs2);

        Map<String, CouchClient.MissingRevisions> diffs = client.revsDiff(revs);
        Assert.assertEquals(2, diffs.size());
        Assert.assertThat(diffs.keySet(), hasItems("A", "B"));

        Assert.assertEquals(2, diffs.get("A").missing.size());
        Assert.assertEquals(2, diffs.get("B").missing.size());

        Assert.assertThat(diffs.get("A").missing, hasItems("1-a", "2-b"));
        Assert.assertThat(diffs.get("B").missing, hasItems("1-c", "2-d"));
    }

    @Test
    public void revsDiff_oneDocsOneRev_returnNothing() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);

        Set<String> revs1 = CollectionFactory.SET.of(res.getRev());
        Map<String, Set<String>> revs = CollectionFactory.MAP.of(res.getId(), revs1);

        Map<String, CouchClient.MissingRevisions> diffs = client.revsDiff(revs);
        Assert.assertEquals(0, diffs.size());
    }

    @Test
    public void revsDiff_oneDocsTwoRev_returnOneRevs() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);

        Set<String> revs1 = CollectionFactory.SET.of(res.getRev(), "2-a");
        Map<String, Set<String>> revs = CollectionFactory.MAP.of(res.getId(), revs1);

        Map<String, CouchClient.MissingRevisions> diffs = client.revsDiff(revs);
        Assert.assertEquals(1, diffs.size());
        Assert.assertEquals(1, diffs.get(res.getId()).missing.size());
        Assert.assertThat(diffs.get(res.getId()).missing, hasItem("2-a"));
    }

    private boolean isDbExist(String dbName) {
        try {
            CouchConfig config = getCouchConfig(dbName);
            CouchClient customClient = new CouchClient(config.getRootUri(), config.getRequestInterceptors(), config.getResponseInterceptors());
            customClient.getDbInfo();
            return true;
        } catch (CouchException ce) {
            return false;
        }
    }
}
