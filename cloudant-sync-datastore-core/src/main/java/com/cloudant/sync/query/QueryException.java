package com.cloudant.sync.query;

import com.cloudant.sync.documentstore.DocumentStoreException;

/**

 * @api_public
 */
public class QueryException extends DocumentStoreException {
    public QueryException(Throwable causedBy){
        super(causedBy);
    }

    public QueryException(String message){
        super(message);
    }

    public QueryException(String message, Throwable causedBy) {
        super(message, causedBy);
    }

}
