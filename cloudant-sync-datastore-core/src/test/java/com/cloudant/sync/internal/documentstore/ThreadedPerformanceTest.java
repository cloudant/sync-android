package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentRevision;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by tomblench on 01/12/2016.
 */

public class ThreadedPerformanceTest extends DatastoreTestBase {

    @Test
    public void doThing1() throws Exception {

        int nThreads = 100;
        int nDocs = 500*1;
        int nTotalDocs = nThreads * nDocs;
        List<Thread> threads = new ArrayList<Thread>();
        List<String> docIds = new ArrayList<String>();

        long start = System.currentTimeMillis();

        // writer thread
        for (int t=0; t<nThreads; t++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int d=0; d<nDocs; d++){
                        DocumentRevision documentRevision = new DocumentRevision();
                        Map<String, String> map = new HashMap<String, String>();
                        for (int k = 0; k < 100; k++) {
                            map.put("key" + k, "value" + k);
                        }
                        documentRevision.setBody(DocumentBodyFactory.create(map));
                        try {
                            docIds.add(datastore.createDocumentFromRevision(documentRevision).getId());
                        } catch (DocumentException de) {
                            System.err.println(de);
                        }
                    }
                }
            });
            threads.add(th);
        }
        Random r = new Random();
        // reader/updater thread
        for (int t=0; t<nThreads; t++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int d=0; d<nDocs; d++){
                        try {
                            DocumentRevision dr = datastore.getDocument(docIds.get(r.nextInt(docIds.size())));
                            dr = datastore.getDocument(docIds.get(r.nextInt(docIds.size())));
                            dr = datastore.getDocument(docIds.get(r.nextInt(docIds.size())));
                            dr = datastore.getDocument(docIds.get(r.nextInt(docIds.size())));
                            Map<String, String> map = new HashMap<String, String>();
                            for (int k = 0; k < 100; k++) {
                                map.put("key2" + k, "value2" + k);
                            }
                            dr.setBody(DocumentBodyFactory.create(map));
                            datastore.updateDocumentFromRevision(dr);
                        } catch (DocumentException de) {
                            System.err.println(de);
                        }
                    }
                }
            });
            threads.add(th);
        }
        for (Thread t : threads) {
            t.run();
        }
        for (Thread t : threads) {
            t.join();
        }

        long end = System.currentTimeMillis();
        float total = ((float)(end-start))/1000.0f;

        System.out.println(String.format("%d total documents across %d threads in %.2f seconds (%.2f seconds per doc or %.2f docs/s)",
                nTotalDocs, nThreads, total, (total/nTotalDocs), (nTotalDocs/total)));


    }



}
