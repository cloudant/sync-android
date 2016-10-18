package com.cloudant.sync.query;

import static org.hamcrest.core.IsEqual.equalTo;

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
