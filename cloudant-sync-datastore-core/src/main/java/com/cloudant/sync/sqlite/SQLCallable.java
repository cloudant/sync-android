package com.cloudant.sync.sqlite;

/**
 * Created by tomblench on 25/08/16.
 *
 * @api_private
 */
public interface SQLCallable<T> {

    T call(SQLDatabase db) throws Exception;

}
