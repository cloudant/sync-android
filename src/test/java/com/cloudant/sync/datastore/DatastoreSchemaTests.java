/**
 * Copyright (c) 2014 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by tomblench on 15/04/2014.
 */
public class DatastoreSchemaTests {

    @Test
    public void testSchema4UuidsDifferent() {
        // privateUuid and publicUuid have to be unique on the info table for each DB
        String[] schema_Alpha = DatastoreConstants.getSchemaVersion4();
        String[] schema_Beta  = DatastoreConstants.getSchemaVersion4();
        assertThat("Schemas should be different", schema_Alpha, not(equalTo(schema_Beta)));
    }

    @Test
    public void testSchemasSame() {
        // for contrast with above test, schema 3 and 5 are identical each time
        String[] schema_Alpha3 = DatastoreConstants.getSchemaVersion3();
        String[] schema_Beta3  = DatastoreConstants.getSchemaVersion3();
        assertThat("Schemas should be the same", schema_Alpha3, equalTo(schema_Beta3));

        String[] schema_Alpha5 = DatastoreConstants.getSchemaVersion5();
        String[] schema_Beta5  = DatastoreConstants.getSchemaVersion5();
        assertThat("Schemas should be the same", schema_Alpha5, equalTo(schema_Beta5));
    }

}
