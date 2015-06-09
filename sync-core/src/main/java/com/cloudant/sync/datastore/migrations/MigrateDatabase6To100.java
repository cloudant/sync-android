/**
 * Copyright (c) 2015 IBM Cloudant. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore.migrations;

import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;

import org.apache.commons.codec.binary.Hex;

/**
 * Migration from datastore JSON database version 6 to 100, which requires migration of data
 * between tables with a transform in Java code along the way.
 *
 * This change is a breaking change because after the migration, older versions of the code
 * will not be able to find attachments on disk because they will look for the path based
 * on the hex of the key rather than going via the attachments table.
 */
public class MigrateDatabase6To100 implements Migration {

    public void runMigration(SQLDatabase db) throws Exception {

        db.execSQL("CREATE TABLE attachments_key_filename ( " +
                "key TEXT UNIQUE NOT NULL, " +
                "filename TEXT UNIQUE NOT NULL);");
        db.execSQL("CREATE INDEX attachments_key_filename_index " +
                "ON attachments_key_filename(key, filename);");

        // Migrate all existing blobs into the attachments_key_filename table
        // using the attachment key as both the key and filename
        String sql = "SELECT key FROM attachments;";
        Cursor c = db.rawQuery(sql, null);
        while (c.moveToNext()) {
            byte[] key = c.getBlob(0);
            String hexKey = new String(Hex.encodeHex(key));
            ContentValues cv = new ContentValues();
            cv.put("key", hexKey);
            cv.put("filename", hexKey);  // existing blobs retain key as filename
            db.insert("attachments_key_filename", cv);
        }

    }
}
