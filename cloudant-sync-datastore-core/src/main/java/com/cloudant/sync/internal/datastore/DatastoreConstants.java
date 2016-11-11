/*
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright © 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright © 2013 Cloudant, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.internal.datastore;

import com.cloudant.sync.internal.util.Misc;

import java.util.HashSet;
import java.util.Set;

/**
 * @api_private
 */
class DatastoreConstants {

    public static final Set<String> KNOWN_SPECIAL_KEYS;

    static {
        KNOWN_SPECIAL_KEYS = new HashSet<String>();
        KNOWN_SPECIAL_KEYS.add("_id");
        KNOWN_SPECIAL_KEYS.add("_rev");
        KNOWN_SPECIAL_KEYS.add("_attachments");
        KNOWN_SPECIAL_KEYS.add("_deleted");
        KNOWN_SPECIAL_KEYS.add("_revisions");
        KNOWN_SPECIAL_KEYS.add("_revs_info");
        KNOWN_SPECIAL_KEYS.add("_conflicts");
        KNOWN_SPECIAL_KEYS.add("_deleted_conflicts");
    }

    // First-time initialization:
    // (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
    // monotonically increasing, never reused. See <http://www.sqlite4java.org/autoinc.html>)
    public static String[] getSchemaVersion3() {
        return new String[] {
            "    CREATE TABLE docs ( " +
            "        doc_id INTEGER PRIMARY KEY, " +
            "        docid TEXT UNIQUE NOT NULL); ",
            "    CREATE INDEX docs_docid ON docs(docid); ",
            "    CREATE TABLE revs ( " +
            "        sequence INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "        doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE, " +
            "        parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL, " +
            "        current BOOLEAN, " +
            "        deleted BOOLEAN DEFAULT 0, " +
            "        available BOOLEAN DEFAULT 1, " +
            "        revid TEXT NOT NULL, " +
            "        json BLOB); ",
            "    CREATE INDEX revs_by_id ON revs(revid, doc_id); ",
            "    CREATE INDEX revs_current ON revs(doc_id, current); ",
            "    CREATE INDEX revs_parent ON revs(parent); ",
            "    CREATE TABLE localdocs ( " +
            "        docid TEXT UNIQUE NOT NULL, " +
            "        revid TEXT NOT NULL, " +
            "        json BLOB); ",
            "    CREATE INDEX localdocs_by_docid ON localdocs(docid); ",
            "    CREATE TABLE views ( " +
            "        view_id INTEGER PRIMARY KEY, " +
            "        name TEXT UNIQUE NOT NULL," +
            "        version TEXT, " +
            "        lastsequence INTEGER DEFAULT 0); ",
            "    CREATE INDEX views_by_name ON views(name); ",
            "    CREATE TABLE maps ( " +
            "        view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, " +
            "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
            "        key TEXT NOT NULL, " +
            "        collation_key BLOB NOT NULL, " +
            "        value TEXT); ",
            "    CREATE INDEX maps_keys on maps(view_id, collation_key); ",
            "    CREATE TABLE attachments ( " +
            "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
            "        filename TEXT NOT NULL, " +
            "        key BLOB NOT NULL, " +
            "        type TEXT, " +
            "        length INTEGER NOT NULL, " +
            "        revpos INTEGER DEFAULT 0); ",
            "    CREATE INDEX attachments_by_sequence on attachments(sequence, filename); ",
            "    CREATE TABLE replicators ( " +
            "        remote TEXT NOT NULL, " +
            "        startPush BOOLEAN, " +
            "        last_sequence TEXT, " +
            "        UNIQUE (remote, startPush)); "
        };
    };

    public static String[] getSchemaVersion4() {
        return new String[] {
            "CREATE TABLE info ( " +
            "    key TEXT PRIMARY KEY, " +
            "    value TEXT); ",
            String.format("INSERT INTO INFO (key, value) VALUES ('privateUUID', '%s'); ", Misc.createUUID()),
            String.format("INSERT INTO INFO (key, value) VALUES ('publicUUID',  '%s'); ", Misc.createUUID())
        };
    };

    public static String[] getSchemaVersion5() {
        return new String[] {
            "ALTER TABLE attachments ADD COLUMN encoding INTEGER DEFAULT 0; ",
            "ALTER TABLE attachments ADD COLUMN encoded_length INTEGER DEFAULT 0; "
        };
    };

    public static String[] getSchemaVersion6(){
        return new String[]{
                "    CREATE TABLE new_localdocs ( " +
                "            docid TEXT UNIQUE NOT NULL, " +
                "            json BLOB); ",
                "    INSERT INTO new_localdocs SELECT docid, json FROM localdocs",
                "    DROP TABLE localdocs",
                "    ALTER TABLE new_localdocs RENAME TO localdocs",
                "    CREATE INDEX localdocs_by_docid ON localdocs(docid); "
        };
    }

    // NB schema version 100 updates are defined in MigrateDatabase6To100 class

    public static String[] getSchemaVersion200() {
        return new String[]{
                // migrate revs to new schema which is identical except for added constraint
                // "UNIQUE (doc_id, revid)"
                "    CREATE TABLE new_revs ( " +
                "        sequence INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "        doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE, " +
                "        parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL, " +
                "        current BOOLEAN, " +
                "        deleted BOOLEAN DEFAULT 0, " +
                "        available BOOLEAN DEFAULT 1, " +
                "        revid TEXT NOT NULL, " +
                "        json BLOB, " +
                "        UNIQUE (doc_id, revid)); ",
                "    INSERT INTO new_revs (sequence, doc_id, parent, current, deleted, available, revid, json) SELECT sequence, doc_id, parent, current, deleted, available, revid, json FROM revs; ",
                "    DROP TABLE revs; ",
                "    ALTER TABLE new_revs RENAME TO revs; ",
                "    CREATE INDEX revs_by_id ON revs(revid, doc_id); ",
                "    CREATE INDEX revs_current ON revs(doc_id, current); ",
                "    CREATE INDEX revs_parent ON revs(parent); ",
                // migrate attachments to new schema which is identical except for added constraint
                // UNIQUE (sequence, filename)
                "    CREATE TABLE new_attachments ( " +
                "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
                "        filename TEXT NOT NULL, " +
                "        key BLOB NOT NULL, " +
                "        type TEXT, " +
                "        length INTEGER NOT NULL, " +
                "        revpos INTEGER DEFAULT 0, " +
                "        encoding INTEGER DEFAULT 0, " +
                "        encoded_length INTEGER DEFAULT 0, " +
                "        UNIQUE (sequence, filename)); ",
                "    INSERT INTO new_attachments (sequence, filename, key, type, length, revpos, encoding, encoded_length) SELECT sequence, filename, key, type, length, revpos, encoding, encoded_length FROM attachments; ",
                "    DROP TABLE attachments; ",
                "    ALTER TABLE new_attachments RENAME to attachments",
                "    CREATE INDEX attachments_by_sequence on attachments(sequence, filename); ",
                "    PRAGMA foreign_key_check; ",
        };
    }

}
