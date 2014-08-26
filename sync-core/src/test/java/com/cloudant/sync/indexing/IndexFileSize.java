package com.cloudant.sync.indexing;

import com.cloudant.common.PerformanceTest;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.util.TestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category(PerformanceTest.class)
public class IndexFileSize {

    String datastoreManagerPath;
    DatastoreManager datastoreManager;
    final int COUNT = 100000;
    final int STEP = 1000;

    public void setUp() throws IOException, SQLException {
        datastoreManagerPath = TestUtils.createTempTestingDir(this.getClass().getName());
        datastoreManager = new DatastoreManager(this.datastoreManagerPath);
    }

    public void tearDown() throws Exception {
        TestUtils.deleteTempTestingDir(datastoreManagerPath);
    }

    @Test
    public void test() throws Exception {
        this.setUp();
        List<Long> withIndex = this.withIndex();
        this.tearDown();

        this.setUp();
        List<Long> withoutIndex = this.withoutIndex();
        this.tearDown();

        int p = 0;
        for(int i = 0 ; i < withIndex.size() ; i ++) {
            long t1 = withIndex.get(i);
            long t2 = withoutIndex.get(i);
            System.out.println(  p + "," + t1 + ", " + t2 + ", " + Double.valueOf(t1)/t2);
            p = p + 1000;
        }
    }

    public List<Long> withIndex() throws Exception {
        Datastore datastore = datastoreManager.openDatastore("dbsize");
        IndexManager indexManager = new IndexManager(datastore);
        indexManager.ensureIndexed("name", "name");
        List<Long> dbSize = new ArrayList<Long>();
        int p = 0 ;
        for(int i = 0 ; i <= COUNT ; i ++) {
            Map m = new HashMap<String, String>();
            m.put("name", "tom");
            m.put("age", 12);
            DocumentBody body = DocumentBodyFactory.create(m);
            BasicDocumentRevision rev = datastore.createDocument(body);

            if(i == p) {
                indexManager.updateAllIndexes();
                File file = new File(datastoreManagerPath + File.separator + "dbsize" + File.separator + "db.sync");
                dbSize.add(file.length());
                System.out.println("{ \"size1\": " + file.length() + ", \"count\":" + i + "}");
                p = p + STEP;
            }
        }

        return dbSize;
    }

    public List<Long> withoutIndex() throws Exception {
        Datastore datastore = datastoreManager.openDatastore("dbsize");
        IndexManager indexManager = new IndexManager(datastore);
        indexManager.ensureIndexed("name", "name");
        List<Long> dbSize = new ArrayList<Long>();
        int p = 0;
        for(int i = 0 ; i <= COUNT ; i ++) {
            Map m = new HashMap<String, String>();
            m.put("name", "tom");
            m.put("age", 12);
            DocumentBody body = DocumentBodyFactory.create(m);
            BasicDocumentRevision rev = datastore.createDocument(body);

            if(i == p) {
                File file = new File(datastoreManagerPath + File.separator + "dbsize" + File.separator + "db.sync");
                System.out.println("{ \"size1\": " + file.length() + ", \"count\":" + i + "}");
                dbSize.add(file.length());
                p = p + STEP;
            }
        }
        return dbSize;
    }
}
