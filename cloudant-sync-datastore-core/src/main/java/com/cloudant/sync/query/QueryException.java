package com.cloudant.sync.query;

/**
 * This unchecked exception is used to wrap another exception so
 * we can pass meaningful exception traces over the {@link QueryResult#iterator()}
 * boundary as we can't change the signature of {@link QueryResult#iterator} to
 * add a checked exception.
 *
 * @api_public
 */
public class QueryException extends RuntimeException {
    public QueryException(Exception causedBy){
        super(causedBy);
    }

    public QueryException(String messaage){
        super(messaage);
    }
}
