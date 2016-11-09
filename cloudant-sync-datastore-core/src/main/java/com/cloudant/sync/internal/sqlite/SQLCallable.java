package com.cloudant.sync.internal.sqlite;

/**
 * Created by tomblench on 25/08/16.
 *
 * @api_private
 */
public interface SQLCallable<T> {

    T call(SQLDatabase db) throws Exception;

}
