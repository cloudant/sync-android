package com.cloudant.common;

import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Before;

import java.io.File;

public abstract class DocumentStoreTestBase {

    protected DocumentStore documentStore;

    protected String datastore_manager_dir;

    @Before
    public void setUpDocumentStore() throws Exception {
        datastore_manager_dir = TestUtils.createTempTestingDir(this.getClass().getName());
        this.documentStore = DocumentStore.getInstance(new File
                (datastore_manager_dir, getClass().getSimpleName()));

    }

    @After
    public void tearDownDocumentStore() {
        documentStore.close();
        TestUtils.deleteTempTestingDir(datastore_manager_dir);
    }
}
