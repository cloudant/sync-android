package com.cloudant.sync.query;

/**
 * Created by tomblench on 28/09/2016.
 */

public class FieldSort {

    public final String field;
    public final Direction sort;

    public FieldSort(String field) {
        this.field = field;
        this.sort = Direction.ASCENDING;
    }

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

    public enum Direction {
        ASCENDING,
        DESCENDING
    };

}
