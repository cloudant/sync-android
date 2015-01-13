//  Copyright (c) 2014 Cloudant. All rights reserved.
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
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import java.util.Arrays;

public class IndexManagerTest extends AbstractIndexTestBase {

    @Test
    public void unimplementedEnsureIndexed() {
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name")), is(nullValue()));
    }

    @Test
    public void unimplementedDeleteIndexNamed() {
        assertThat(im.deleteIndexNamed("basic"), is(false));
    }
}