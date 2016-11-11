/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.query;

import static org.hamcrest.core.IsEqual.equalTo;

import com.cloudant.sync.query.Index;
import com.cloudant.sync.internal.query.FieldSort;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tomblench on 12/10/2016.
 */

public class IndexMatcherHelpers {

    // hamcrest matcher to map from an index to its name
    public static Matcher<Index> getIndexNameMatcher(String index) {
        return new FeatureMatcher<Index, String>(equalTo(index), "indexName", "indexName") {
            @Override
            protected String featureValueOf(Index i) {
                return i.indexName;
            }
        };
    }

    // hamcrest matcher to assert that an index has a given list of field names
    public static Matcher<Index> hasFieldsInAnyOrder(String... index) {
        return new FeatureMatcher<Index, List<String>>(Matchers.containsInAnyOrder(index),
                "fields", "fields") {
            @Override
            protected List<String> featureValueOf(Index i) {
                ArrayList fields = new ArrayList<String>();
                for (FieldSort f : i.fieldNames) {
                    fields.add(f.field);
                }
                return fields;
            }
        };
    }

    // helper to return an index with a given name from a list
    public static Index getIndexNamed(String name, List<Index> indexes) {
        for (Index i : indexes) {
            if (i.indexName.equals(name)) {
                return i;
            }
        }
        return null;
    }

}
