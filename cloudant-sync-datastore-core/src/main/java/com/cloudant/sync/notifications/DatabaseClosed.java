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

/**
 * <p>Event for DocumentStore closed.</p>
 *
 * <p>This event is posted by {@link DocumentStore#close()}</p>
 *
 * @api_public
 */
public class DatabaseClosed extends DatabaseModified {

    /**
     * Event for DocumentStore closed
     *
     * @param dbName
     *            The name of the DocumentStore that was closed
     */
    public DatabaseClosed(String dbName) {
        super(dbName);
    }
}