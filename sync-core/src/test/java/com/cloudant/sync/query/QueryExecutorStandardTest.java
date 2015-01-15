//  Copyright (c) 2015 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * This class uses the {@link IndexManager}, to allow all tests defined in
 * {@link AbstractQueryTestBase} to run in the standard manner that queries
 * would normally execute in a production setting.
 *
 * @see IndexManager
 * @see AbstractQueryTestBase
 */
public class QueryExecutorStandardTest extends AbstractQueryTestBase {
    @Override
    public void setUp() throws SQLException {
        super.setUp();
        im = new IndexManager(ds);
        assertThat(im, is(notNullValue()));
        db = TestUtils.getDatabaseConnectionToExistingDb(im.getDatabase());
        assertThat(db, is(notNullValue()));
        assertThat(im.getQueue(), is(notNullValue()));
        String[] metadataTableList = new String[] { IndexManager.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(db, metadataTableList);

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet"), "pet"), is("pet"));
    }
}
