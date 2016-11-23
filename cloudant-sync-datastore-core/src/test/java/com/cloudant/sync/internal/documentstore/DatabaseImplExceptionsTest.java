package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;

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

    // getDocument on non-existent document should throw DocumentNotFoundException
    @Test(expected = DocumentNotFoundException.class)
    public void getDocumentShouldThrowDocumentNotFoundException() throws Exception {
        this.datastore.getDocument("nosuchdocument");
    }

    // getDocument after we remove the underlying SQL database should throw DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getDocumentShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getDocument("nosuchdocument");
    }

    // getDocument on non-existent revid should throw DocumentNotFoundException
    @Test(expected = DocumentNotFoundException.class)
    public void getDocumentWithRevisionShouldThrowDocumentNotFoundException() throws Exception {
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.datastore.createDocumentFromRevision(dr);
        this.datastore.getDocument("nosuchdocument", "nosuchrevid");
    }

    // getDocument after we remove the underlying SQL database should throw DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getDocumentWithRevisionShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getDocument("nosuchdocument", "nosuchrevid");
    }

    // containsDocument after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void containsDocumentShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.containsDocument("nosuchdocument");
    }

    // containsDocument after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void containsDocumentWithRevisionShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.containsDocument("nosuchdocument", "nosuchrevid");
    }

    // getAllDocuments after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getAllDocumentsShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getAllDocuments(0, 100, true);
    }

    // getAllDocumentIds after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getAllDocumentIdsShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getAllDocumentIds();
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

    // getConflictedDocumentIds after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void getConflictedDocumentIdsShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.getConflictedDocumentIds();
    }

    // createDocumentFromRevision after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void createDocumentFromRevisionShouldThrowDocumentStoreException() throws Exception {
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.datastore.createDocumentFromRevision(dr);
    }

    // updateDocumentFromRevision after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void updateDocumentFromRevisionShouldThrowDocumentStoreException() throws Exception {
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        dr = this.datastore.createDocumentFromRevision(dr);
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"updated\"}".getBytes()));
        this.datastore.updateDocumentFromRevision(dr);
    }

    // updateDocumentFromRevision after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void deleteDocumentFromRevisionShouldThrowDocumentStoreException() throws Exception {
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        dr = this.datastore.createDocumentFromRevision(dr);
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.deleteDocumentFromRevision(dr);
    }

    // updateDocumentFromRevision after we remove the underlying SQL database should throw
    // DocumentStoreException
    @Test(expected = DocumentStoreException.class)
    public void deleteDocumentShouldThrowDocumentStoreException() throws Exception {
        DocumentRevision dr = new DocumentRevision("doc1");
        dr.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        dr = this.datastore.createDocumentFromRevision(dr);
        // remove the underlying database, triggering a SQL Exception
        dropRevs();
        this.datastore.deleteDocument(dr.getId());
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