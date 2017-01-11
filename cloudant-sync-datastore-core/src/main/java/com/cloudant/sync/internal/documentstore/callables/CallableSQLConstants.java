package com.cloudant.sync.internal.documentstore.callables;

/**
 * Collection of SQL Constants for use with Callables.
 */
final class CallableSQLConstants {

    // for building up the strings below
    static final String METADATA_COLS = "docs.docid, docs.doc_id, revid, sequence, " +
            "current, deleted, parent";

    static final String FULL_DOCUMENT_COLS = METADATA_COLS + ", json";

    static final String CURRENT_REVISION_CLAUSES =
            "FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND current=1 ORDER " +
                    "BY revid DESC LIMIT 1";

    private static final String GIVEN_REVISION_CLAUSES =
            "FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? ORDER BY " +
                    "revs.sequence LIMIT 1";

    // get all document columns for current ("winning") revision of a given doc ID
    static final String GET_DOCUMENT_CURRENT_REVISION =
            "SELECT " + FULL_DOCUMENT_COLS + " " + CURRENT_REVISION_CLAUSES;

    // get metadata (everything except json) for current ("winning") revision of a given doc ID
    static final String GET_METADATA_CURRENT_REVISION =
            "SELECT " + METADATA_COLS + " " + CURRENT_REVISION_CLAUSES;

    // get all document columns for a given revision and doc ID† (see below)
    static final String GET_DOCUMENT_GIVEN_REVISION =
            "SELECT " + FULL_DOCUMENT_COLS + " " + GIVEN_REVISION_CLAUSES;

    // get metadata (everything except json) for a given revision and doc ID† (see below)
    static final String GET_METADATA_GIVEN_REVISION =
            "SELECT " + METADATA_COLS + " " + GIVEN_REVISION_CLAUSES;

    static final String GET_DOC_NUMERIC_ID = "SELECT doc_id from docs WHERE docid=?";

    static final String SQL_CHANGE_IDS_SINCE_LIMIT = "SELECT doc_id, max(sequence) FROM " +
            "revs WHERE sequence > ? AND sequence <= ? GROUP BY doc_id ";

    // † N.B. whilst there should only ever be a single result bugs have resulted in duplicate
    // revision IDs in the tree. Whilst it appears that the lowest sequence number is always
    // returned by these queries we use ORDER BY sequence to guarantee that and lock down a
    // behaviour for any future occurrences of duplicate revs in a tree.

    private CallableSQLConstants() {
        // empty
    }

}
