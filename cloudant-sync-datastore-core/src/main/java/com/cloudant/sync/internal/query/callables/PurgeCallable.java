package com.cloudant.sync.internal.query.callables;

import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;

/**
 * Created by tomblench on 29/08/2017.
 */

public class PurgeCallable implements SQLCallable<Void> {

    private String documentId;
    private String revisionId;

    public PurgeCallable(String documentId, String revisionId) {
        this.documentId = documentId;
        this.revisionId = revisionId;
    }

    @Override
    public Void call(SQLDatabase db) throws Exception {
        System.out.println("PurgeCallable query");
        // TODO
        return null;
    }

}
