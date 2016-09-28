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


    public enum Direction {
        ASCENDING,
        DESCENDING
    };


}
