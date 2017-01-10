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

package com.cloudant.sync.query;

/**
 * Created by tomblench on 28/09/2016.
 */

/**
 * The {@code FieldSort} object is used to specify field names and a sort direction
 * (ascending/descending) for index creation and querying.
 *
 * @api_public
 */
public class FieldSort {

    /**
     * The field name
     */
    public final String field;

    /**
     * The sort direction
     */
    public final Direction sort;

    /**
     * Returns a field sort specification with the given field name and the default direction of
     * {@link Direction#ASCENDING}
     * @param field the field name
     */
    public FieldSort(String field) {
        this(field, Direction.ASCENDING);
    }

    /**
     * Returns a field sort specification with the given field name and direction
     * @param field the field name
     * @param sort the sort direction
     */
    public FieldSort(String field, Direction sort) {
        this.field = field;
        this.sort = sort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldSort fieldSort = (FieldSort) o;

        if (!field.equals(fieldSort.field)) {
            return false;
        }
        return sort == fieldSort.sort;

    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + sort.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "FieldSort{" +
                "field='" + field + '\'' +
                ", sort=" + sort +
                '}';
    }

    /**
     * Sort direction for fields
     */
    public enum Direction {
        /**
         * Ascending sort direction
         */
        ASCENDING,
        /**
         * Descending sort direction
         */
        DESCENDING
    };

}
