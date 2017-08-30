package com.cloudant.sync.internal.documentstore;

import com.cloudant.common.DocumentStoreTestBase;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.query.FieldSort;

import org.junit.Test;

import java.util.Collections;

/**
 * Created by tomblench on 29/08/2017.
 */

public class PurgeTest extends BasicDatastoreTestBase {

    @Test
    public void purge() throws Exception {
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(bodyOne);
        DocumentRevision rev2 = this.documentStore.database().create(rev);
        this.documentStore.query().createJsonIndex(Collections.singletonList(new FieldSort("Sunrise")), null);
        this.documentStore.query().createJsonIndex(Collections.singletonList(new FieldSort("Sunset")), null);
        this.documentStore.database().purge(rev2);
    }
}
