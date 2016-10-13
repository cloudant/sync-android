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

public class MatcherHelper {

    public static Matcher<Index> getIndexNameMatcher(String index) {
        return new FeatureMatcher<Index, String>(equalTo(index), "indexName", "indexName") {
            @Override
            protected String featureValueOf(Index i) {
                return i.indexName;
            }
        };
    }

    public static Index getIndexNamed(String name, List<Index> indexes) {
        for (Index i : indexes) {
            if (i.indexName.equals(name)) {
                return i;
            }
        }
        return null;
    }

    public static Matcher<Index> getFields(String... index) {
        return new FeatureMatcher<Index, List<String>>(Matchers.containsInAnyOrder(index), "fields", "fields") {
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



}
