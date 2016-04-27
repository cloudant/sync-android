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

/**
 * <p>Event for database created, it is only posted when the database
 * is created on the disk.</p>
 *
 *
 * <p>This event is posted by
 * {@link com.cloudant.sync.datastore.DatastoreManager#openDatastore(String) openDatastore(String)}
 * </p>
 *
 * @api_public
 */
public class DatabaseCreated extends DatabaseModified {

    /**
     * Event for database created for the first time.
     *
     * @param dbName
     *            The name of the datastore that was created
     */
    public DatabaseCreated(String dbName) {
        super(dbName);
    }

}
