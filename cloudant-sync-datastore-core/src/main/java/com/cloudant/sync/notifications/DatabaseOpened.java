/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.DocumentStore;
import com.cloudant.sync.datastore.encryption.KeyProvider;

import java.io.File;

/**
 * <p>Event for DocumentStore opened.</p>
 *
 * <p>This event is posted the first time a {@link com.cloudant.sync.datastore.DocumentStore}
 * is opened during the lifetime of a program; or if {@link DocumentStore#close()} is called and a
 * DocumentStore is subsequently re-opened.</p>
 *
 * <p>This event is posted by
 * {@link com.cloudant.sync.datastore.DocumentStore#getInstance(File)} and
 * {@link com.cloudant.sync.datastore.DocumentStore#getInstance(File, KeyProvider)}
 * </p>
 *
 * @api_public
 */
public class DatabaseOpened extends DatabaseModified {

    /**
     * Event for DocumentStore opened.
     *
     * @param dbName
     *            The name of the DocumentStore that was opened
     */
    public DatabaseOpened(String dbName) {
        super(dbName);
    }

}
