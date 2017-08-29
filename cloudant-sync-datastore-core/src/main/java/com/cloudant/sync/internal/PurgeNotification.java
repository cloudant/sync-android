package com.cloudant.sync.internal;

import com.cloudant.sync.event.notifications.Notification;

/**
 * Created by tomblench on 29/08/2017.
 */

public class PurgeNotification implements Notification {

    public final String documentId;
    public final String revisionId;

    public PurgeNotification(String documentId, String revisionId) {
        this.documentId = documentId;
        this.revisionId = revisionId;
    }
}