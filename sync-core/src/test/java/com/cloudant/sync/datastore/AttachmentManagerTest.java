/**
 * Copyright (c) 2015 IBM Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;

import org.junit.Assert;

import org.junit.Test;

import java.io.File;
import java.sql.SQLException;

/**
 * Test AttachmentManager classes
 */
public class AttachmentManagerTest {

    /**
     * Test that name read from database is returned
     */
    @Test
    public void testFileForNameFindsName() throws AttachmentException, SQLException {

        final String my_filename = "my_filename";

        // Force returning my_filename
        Cursor c = mock(Cursor.class);
        when(c.moveToFirst()).thenReturn(true);
        when(c.getString(anyInt())).thenReturn(my_filename);  // make cursor returning my_filename
        SQLDatabase db = mock(SQLDatabase.class);
        when(db.rawQuery(anyString(), any(String[].class))).thenReturn(c);  // use that cursor

        // Mock enough datastore for AttachmentManager
        BasicDatastore ds = mock(BasicDatastore.class);
        when(ds.extensionDataFolder(anyString())).thenReturn("blah");
        when(ds.getKeyProvider()).thenReturn(new NullKeyProvider());

        File expected = new File("blah", my_filename);

        // Test
        AttachmentManager attachmentManager = new AttachmentManager(ds);
        Assert.assertEquals(expected, attachmentManager.fileFromKey(db, new byte[]{-1, 23}));

    }

    /**
     * Test that name is generated when it cannot be read from the database.
     */
    @Test
    public void testFileForNameGeneratesName() throws AttachmentException, SQLException {

        final String my_filename = "my_filename";

        // Force returning my_filename
        Cursor c = mock(Cursor.class);
        when(c.moveToFirst()).thenReturn(false);  // no existing filename
        SQLDatabase db = mock(SQLDatabase.class);
        when(db.rawQuery(anyString(), any(String[].class))).thenReturn(c);  // no existing filename
        when(db.insert(anyString(), any(ContentValues.class))).thenReturn(10l);  // 10 => success

        // Mock enough datastore for AttachmentManager
        BasicDatastore ds = mock(BasicDatastore.class);
        when(ds.extensionDataFolder(anyString())).thenReturn("blah");
        when(ds.getKeyProvider()).thenReturn(new NullKeyProvider());

        // Test
        AttachmentManager attachmentManager = new AttachmentManager(ds);
        final File fileFromKey = attachmentManager.fileFromKey(db, new byte[]{-1, 23});
        Assert.assertEquals(new File("blah"), fileFromKey.getParentFile());
        Assert.assertNotNull(fileFromKey.getName());
        Assert.assertTrue(fileFromKey.getName().length() == 40);
    }

    /**
     * Tests that generateFilenameForKey returns a filename when
     * inserting into the database succeeds.
     */
    @Test
    public void testNameGeneration() throws AttachmentManager.NameGenerationException {
        SQLDatabase db = mock(SQLDatabase.class);
        when(db.insert(anyString(), any(ContentValues.class))).thenReturn(10l);  // 10 => success
        Assert.assertNotNull(AttachmentManager.generateFilenameForKey(db, "blah"));
    }

    /**
     * Tests that generateFilenameForKey throws an exception when it is never
     * able to find a suitable filename (i.e., insert always fails).
     */
    @Test
    public void testNameGenerationFailure() throws AttachmentManager.NameGenerationException {
        SQLDatabase db = mock(SQLDatabase.class);
        when(db.insert(anyString(), any(ContentValues.class))).thenReturn(-1l);  // -1 => failure

        // Catch exception so we can verify number of insert calls
        try {
            AttachmentManager.generateFilenameForKey(db, "blah");
            Assert.fail("Exception not thrown when name not generated");
        } catch (AttachmentManager.NameGenerationException ex) {
            verify(db, times(200)).insert(anyString(), any(ContentValues.class));
        }

    }

}
