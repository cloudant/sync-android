/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 */

package com.cloudant.sync.internal.documentstore;

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
