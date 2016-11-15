package com.cloudant.sync.documentstore;

import com.cloudant.sync.internal.common.ChangeNotifyingMap;
import com.cloudant.sync.internal.common.SimpleChangeNotifyingMap;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.query.QueryImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tomblench on 14/11/2016.
 */

public class DocumentRevision {

    /**
     * top level key: _id
     */
    protected final String id;

    /**
     * top level key: _rev
     */
    protected String revision;

    /**
     * top level key: _deleted
     */
    protected boolean deleted;

    protected boolean current;

    /**
     * top level key: _attachments
     */
    protected ChangeNotifyingMap<String, Attachment> attachments = SimpleChangeNotifyingMap.wrap(new
            HashMap<String, Attachment>());

    /**
     * the rest of the document
     */
    protected DocumentBody body;

    protected boolean bodyModified = false;

    protected boolean fullRevision = true;

    public DocumentRevision() {
        // BasicDatastore#createDocumentFromRevision will assign an id
        this(null);
    }

    public DocumentRevision(String id) {
        this(id, null);
    }

    public DocumentRevision(String id, String revision) {
        this(id,revision, null);
    }

    public DocumentRevision(String docId, String revId, DocumentBody body) {
        this.id = docId;
        this.revision = revId;
        this.body = body;
    }

    /**
     * @return the unique identifier of the document
     */
    public String getId() {
        return id;
    }

    /**
     * @return the revision ID of this document revision
     */
    public String getRevision() {
        return revision;
    }

    /**
     * @return {@code true} if this revision is marked deleted.
     */
    public boolean isDeleted() {
        return deleted;
    }

    /**
     * @return {@code true} if this revision is the current winner for the
     * document.
     */
    public boolean isCurrent(){
        return current;
    }

    public Map<String, Attachment> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, Attachment> attachments) {
        if (attachments != null) {
            this.attachments = SimpleChangeNotifyingMap.wrap(attachments);
        } else {
            // user cleared the dict, we don't want our notifying map to try to forward to null
            this.attachments = null;
        }
        // user reset the whole attachments dict, this is a change
        this.bodyModified = true;
    }

    public DocumentBody getBody() {
        return body;
    }

    public void setBody(DocumentBody body) {
        this.body = body;
        this.bodyModified = true;
    }



    /**
     * Returns true if this revision is a full revision. A full revision is a revision which
     * contains all the data related to the revision. For example
     * revisions returned from query where only select fields have been included
     * is <strong>not</strong> regarded as a full revision.
     *
     * @return {@code true} if this revision is a full revision.
     */
    public boolean isFullRevision(){
        return fullRevision;
    }





    /**
     * Returns a "Full" document revision.
     *
     * A full document revision is a revision which contains all the information associated with it.
     * For example when a document is returned from
     * {@link QueryImpl#find(Map, long, long, List, List)} using the
     * {@code fields} option to limit the fields returned, a revision will be missing data so it
     * cannot be regarded as a full revision. If the document is a full revision, this method will
     * only attempt to load the full revision from the datastore if {@link InternalDocumentRevision#isFullRevision()}
     * returns false.
     *
     * @return A "Full" document revision.
     * @throws DocumentNotFoundException Thrown if the full document cannot be loaded from the
     * datastore.
     */
    public DocumentRevision toFullRevision() throws DocumentNotFoundException {
        return this;
    }

}
