/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;

import org.junit.Test;

import java.sql.SQLException;

/**
 * Created by tomblench on 23/11/2016.
 */


// tests to check that the correct exceptions get thrown for each public API call eg when the
// underlying DB has had critical tables dropped

public class DatabaseImplExceptionsTest extends BasicDatastoreTestBase {

    private void dropRevs() {
        this.datastore.runOnDbQueue(new SQLCallable<Void>() {
            public Void call(SQLDatabase db) {
                try {
                    db.execSQL("DROP TABLE revs;");
                } catch (SQLException sqe) {
                    ;
                }
                return null;
            }
        });
    }

    // get on non-existent document should throw DocumentNotFoundException
    @Test(expected = DocumentNotFoundException.class)
    public void getDocumentShouldThrowDocumentNotFoundException() throws Exception {
        this.datastore.read("nosuchdocument");
    }

    // get after we remove the underlying SQL database should throw DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getDocumentShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.read("nosuchdocument");
    }

    // get on non-existent revid should throw DocumentNotFoundException
    @Test(expected = DocumentNotFoundException.class)
    public void getDocumentWithRevisionShouldThrowDocumentNotFoundException() throws Exception {
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.datastore.create(dr);
        this.datastore.read(dr.getId(), "nosuchrevid");
    }

    // get after we remove the underlying SQL database should throw DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getDocumentWithRevisionShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.read("nosuchdocument", "nosuchrevid");
    }

    // contains after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void containsDocumentShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.contains("nosuchdocument");
    }

    // contains after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void containsDocumentWithRevisionShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.contains("nosuchdocument", "nosuchrevid");
    }

    // get after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getAllDocumentsShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.read(0, 100, true);
    }

    // getIds after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getAllDocumentIdsShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getIds();
    }

    // getLastSequence after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getLastSequenceShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getLastSequence();
    }

    // getDocumentCount after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getDocumentCountShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getDocumentCount();
    }

    // getConflictedIds after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getConflictedDocumentIdsShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getConflictedIds();
    }

    // create after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void createDocumentFromRevisionShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.datastore.create(dr);
    }

    // update after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void updateDocumentFromRevisionShouldThrowDocumentStoreException() throws Exception {
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        dr = this.datastore.create(dr);
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"updated\"}".getBytes()));
        this.datastore.update(dr);
    }

    // delete after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void deleteDocumentFromRevisionShouldThrowDocumentStoreException() throws Exception {
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        dr = this.datastore.create(dr);
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.delete(dr);
    }

    // delete after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void deleteDocumentShouldThrowDocumentStoreException() throws Exception {
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        dr = this.datastore.create(dr);
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.delete(dr.getId());
    }

    @Test(expected = DocumentStoreException.class)
    public void changesShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.changes(0, 1000);
    }

    @Test(expected = DocumentStoreException.class)
    public void compactShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.compact();
    }


}