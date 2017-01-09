/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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

package com.cloudant.sync.internal.replication;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.collection.IsIn.isIn;

import com.cloudant.sync.internal.common.CouchConstants;
import com.cloudant.sync.internal.mazha.ChangesResult;
import com.cloudant.sync.internal.mazha.CouchClient;
import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.mazha.OkOpenRevision;
import com.cloudant.sync.internal.mazha.OpenRevision;
import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.documentstore.DocumentRevisionTree;
import com.cloudant.sync.internal.documentstore.RevisionHistoryHelper;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Used to assert a pull/push replication results are correct, basically used to assert a CouchDB instance has the
 * same data as a local datastore.
 */
public class DatabaseAssert {

    static final String LOG_TAG = "DatabaseAssert";
    static final int BATCH_LIMIT = 1000;
    private static final Logger logger = Logger.getLogger(DatabaseAssert.class.getCanonicalName());

    /**
     * Provide a standard interface to either DocumentRevision or ChangesResult.Row to
     * avoid duplicate code.
     */
    static class ChangeRowAdaptor {

        final String id;
        final boolean deleted;

        final DocumentRevision documentRevision;
        final ChangesResult.Row row;

        public ChangeRowAdaptor(DocumentRevision object) {
            this.id = object.getId();
            this.deleted = object.isDeleted();

            this.documentRevision = object;
            this.row = null;
        }

        public ChangeRowAdaptor(ChangesResult.Row row) {
            this.id = row.getId();
            this.deleted = row.isDeleted();

            this.documentRevision = null;
            this.row = row;
        }

        public String[] openRevisions(DatabaseImpl datastore) {
            if (documentRevision != null) {
                return getAllOpenRevisions(datastore, this.id);
            } else if (row != null) {
                return openRevisionsFromChangeRow(row).toArray(new String[]{});
            }
            return null;
        }
    }

    /**
     * Assert that all the documents in the local datastore has been pushed to the remote CouchDB,
     * along with all the open revision, revision history.
     *
     * @param datastore
     * @param client
     */
    public static void assertPushed(DatabaseImpl datastore, CouchClient client) throws Exception {
        logger.entering("DatabaseAssert","assertPushed",new Object[]{datastore,client});
        Long start = System.currentTimeMillis();

        Set<String> alreadyChecked = new HashSet<String>();
        Changes changesBatch = datastore.changes(0, BATCH_LIMIT);
        while(changesBatch.size() > 0) {

            for (DocumentRevision object : changesBatch.getResults()) {
                ChangeRowAdaptor adaptor = new ChangeRowAdaptor(object);
                if (alreadyChecked.contains(adaptor.id)) {
                    continue;
                }
                DatabaseAssert.checkChangeRow(adaptor, client, datastore);
                alreadyChecked.add(adaptor.id);
            }

            changesBatch = datastore.changes(changesBatch.getLastSequence(), BATCH_LIMIT);
        }

        Long end = System.currentTimeMillis();
        logger.finest(String.format("Done check took %1 seconds",(end - start)/1000.0));
    }

    /**
     * Assert that all the documents along with its open revision, revision history has been pulled from the remote
     * CouchDb to local datastore.
     *
     * As for now, it skips asserts about attachments.
     *
     * @param client
     * @param datastore
     */
    public static void assertPulled(CouchClient client, DatabaseImpl datastore) throws Exception {
        logger.entering("DatabaseAssert","assertPulled", new Object[]{client,datastore});
        Long start = System.currentTimeMillis();

        Set<String> alreadyChecked = new HashSet<String>();
        ChangesResultWrapper changesBatch = new ChangesResultWrapper(client.changes("0", BATCH_LIMIT));
        while(changesBatch.size() > 0) {

            for(ChangesResult.Row row : changesBatch.getResults()) {
                ChangeRowAdaptor adaptor = new ChangeRowAdaptor(row);
                if(alreadyChecked.contains(adaptor.id)) {
                    continue;
                }
                DatabaseAssert.checkChangeRow(adaptor, client, datastore);
                alreadyChecked.add(adaptor.id);
            }

            changesBatch = new ChangesResultWrapper(client.changes(changesBatch.getLastSeq(), BATCH_LIMIT));
        }

        Long end = System.currentTimeMillis();
        logger.finest(String.format("Done check took %1 seconds",(end - start)/1000.0));
    }

    /**
     * Check the document referred to by a changes feed row is correct on both the local and
     * remote databases.
     *
     * @param document
     * @param client
     * @param datastore
     */
    private static void checkChangeRow(ChangeRowAdaptor document,
                                       CouchClient client, DatabaseImpl datastore) throws Exception{
        String id = document.id;

        logger.info("Checking document: "+id);

        String[] openRevisions = document.openRevisions(datastore);

        // Assert all the open revisions are pulled or pushed
        DatabaseAssert.checkOpenRevisionsAreIdentical(id, openRevisions, datastore, client);

        // Assert the current revision are the same: they should be either all deleted,
        // or the current winning revision have the same content.
        if (document.deleted) {
            DatabaseAssert.checkBothDeleted(id, datastore, client);
        } else {
            DatabaseAssert.checkWinningRevisionSame(id, datastore, client);
        }
    }

    /**
     * Assert the specified document is deleted in both remote CouchDb and local datastore.
     */
    static void checkBothDeleted(String id, DatabaseImpl datastore, CouchClient client) throws Exception {
        DocumentRevision documentRevision = datastore.read(id);
        Assert.assertTrue(documentRevision.isDeleted());

        Map<String, Object> m = client.getDocument(id, documentRevision.getRevision());
        Assert.assertTrue(m.containsKey(CouchConstants._deleted));
        Assert.assertTrue((Boolean)m.get(CouchConstants._deleted));
    }

    /**
     * Assert the open revisions are the same on both remote CouchDb and local datastore. The "openRevision" is from
     * source database, which is remote db for pull, local db for push.
     */
    static void checkOpenRevisionsAreIdentical(String documentId, String[] openRevisions,
                                               DatabaseImpl datastore, CouchClient client) {
        boolean pullAttachmentsInline = false;

        ArrayList<String> attsSince = new ArrayList<String>();

        List<DocumentRevs> documentRevsList = convertToDocumentRevs(
                client.getDocWithOpenRevisions(documentId, Arrays.asList(openRevisions), attsSince, pullAttachmentsInline));

        DocumentRevisionTree tree = datastore.getAllRevisionsOfDocument(documentId);
        Assert.assertNotNull(tree);

        // Assert the open revisions are the same
        List<String> revisionIdsRemote = getOpenRevisions(documentRevsList);
        Set<String> revisionIdsLocal = tree.leafRevisionIds();

        Assert.assertThat(revisionIdsRemote, hasItems(revisionIdsLocal.toArray(new String[]{})));
        Assert.assertEquals(revisionIdsRemote.size(), revisionIdsLocal.size());

        // Assert the path are the same for each open revision
        Map<String, List<String>> pathMapRemote = getRevisionPathMap(documentRevsList);
        Map<String, List<String>> pathMapLocal = getRevisionPathMap(tree);

        Assert.assertEquals(pathMapRemote.size(), pathMapLocal.size());

        for(String key : pathMapRemote.keySet()) {
            Assert.assertThat(pathMapRemote.get(key), equalTo(pathMapLocal.get(key)));
        }
    }

    private static List<DocumentRevs> convertToDocumentRevs(List<OpenRevision> openRevisionList) {
        List<DocumentRevs> documentRevsList = new ArrayList<DocumentRevs>();
        for(OpenRevision openRevision : openRevisionList) {
            documentRevsList.add(((OkOpenRevision)openRevision).getDocumentRevs());
        }
        return documentRevsList;
    }

    /**
     * Assert the current document are the same in both remote CouchDB and local datastore.
     */
    static void checkWinningRevisionSame(String documentId, DatabaseImpl datastore,
                                         CouchClient client) throws Exception{
        Map<String, Object> doc1 = ((DocumentRevision)datastore.read(documentId)).getBody().asMap();
        Map<String, Object> doc2 = client.getDocument(documentId);
        doc2.remove(CouchConstants._attachments);
        DatabaseAssert.assertSameStringMap(doc1, doc2);
    }

    /**
     * Get the map from: open revision -> path, which is a list of revision from the revision to
     * the root, aka backwards.
     */
    static Map<String, List<String>> getRevisionPathMap(List<DocumentRevs> documentRevsList) {
        Map<String, List<String>> res = new HashMap<String, List<String>>();
        for(DocumentRevs documentRevs : documentRevsList) {
            res.put(documentRevs.getRev(), RevisionHistoryHelper.getRevisionPath(documentRevs));
        }
        return res;
    }

    static Map<String, List<String>> getRevisionPathMap(DocumentRevisionTree tree) {
        Map<String, List<String>> res = new HashMap<String, List<String>>();
        for(DocumentRevisionTree.DocumentRevisionNode revision : tree.leafs()) {
            List<String> path = new ArrayList<String>();
            for(DocumentRevision object : tree.getPathForNode(revision.getData().getSequence())) {
                path.add(object.getRevision());
            }
            res.put(revision.getData().getRevision(), path);
        }
        return res;
    }

    static List<String> getOpenRevisions(List<DocumentRevs> documentRevsList) {
        List<String> revisionIds = new ArrayList<String>();
        for(DocumentRevs documentRevs : documentRevsList) {
            revisionIds.add(documentRevs.getRev());
        }
        return revisionIds;
    }

    /**
     * Assert the two String Map are the same.
     */
    static void assertSameStringMap(Map<String, Object> map1, Map<String, Object> map2) {
        Assert.assertThat(map1.keySet(), equalTo(map2.keySet()));
        Assert.assertThat(map1.entrySet(), everyItem(isIn(map2.entrySet())));
        Assert.assertThat(map2.entrySet(), everyItem(isIn(map1.entrySet())));
    }

    private static String[] getAllOpenRevisions(DatabaseImpl datastore, String objectId) {
        DocumentRevisionTree tree = datastore.getAllRevisionsOfDocument(objectId);
        List<String> list = new ArrayList<String>();
        list.addAll(tree.leafRevisionIds());
        return list.toArray(new String[]{});
    }

    static Set<String> openRevisionsFromChangeRow(ChangesResult.Row row) {
        List<ChangesResult.Row.Rev> revisions = row.getChanges();
        Set<String> openRevs = new HashSet<String>(revisions.size());
        for(ChangesResult.Row.Rev rev : revisions) {
            openRevs.add(rev.getRev());
        }
        return openRevs;
    }
}
