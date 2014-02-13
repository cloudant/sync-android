/**
 * Code adapted from:
 *
 * http://stackoverflow.com/questions/8235255/global-property-filter-in-jackson
 */

package com.cloudant.common;

import com.fasterxml.jackson.annotation.JsonFilter;

@JsonFilter(PropertyFilterMixIn.SIMPLE_FILTER_NAME)
public class PropertyFilterMixIn {

    public static final String SIMPLE_FILTER_NAME = "couchKeywordsFilter";

}
