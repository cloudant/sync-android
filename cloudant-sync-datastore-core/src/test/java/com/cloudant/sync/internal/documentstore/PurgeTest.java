package com.cloudant.sync.internal.documentstore;

import com.cloudant.common.DocumentStoreTestBase;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentRevision;

import org.junit.Test;

/**
 * Created by tomblench on 29/08/2017.
 */

public class PurgeTest extends BasicDatastoreTestBase {


    @Test
    public void purge() throws Exception {
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(bodyOne);
        DocumentRevision rev2 = this.documentStore.database().create(rev);
        this.documentStore.database().purge(rev2);
    }
}
