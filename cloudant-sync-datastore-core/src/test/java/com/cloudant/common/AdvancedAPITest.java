package com.cloudant.common;

import com.cloudant.sync.documentstore.advanced.Database;

import org.junit.Before;

/**
 * Superclass that exposes the "advanced" Database APIs for testing
 */
public class AdvancedAPITest extends DocumentStoreTestBase {

    protected Database advancedDatabase;

    @Before
    public void setUpAdvanced() throws Exception {
        advancedDatabase = documentStore.advanced();
    }


}
