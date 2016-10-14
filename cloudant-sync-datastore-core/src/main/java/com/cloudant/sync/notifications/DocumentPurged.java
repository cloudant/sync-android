package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.DocumentRevision;

/**
 * Created by tomblench on 28/09/2016.
 */

public class DocumentPurged extends DocumentModified {

    public DocumentPurged(DocumentRevision document) {
        super(document, null);
    }

}
