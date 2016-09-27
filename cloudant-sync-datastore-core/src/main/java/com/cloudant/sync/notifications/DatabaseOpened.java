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

import com.cloudant.sync.datastore.Database;

/**
 * <p>Event for database opened.</p>
 *
 * <p>The event is only posted the first time a database is opened for
 * a given {@link com.cloudant.sync.datastore.DatastoreManager}</p>
 *
 * <p>When closed Database (by calling {@link Database#close()})
 * is opened again, the event is also fired.</p>
 *
 * <p>This event is posted by
 * {@link com.cloudant.sync.datastore.DatastoreManager#openDatastore(String) openDatastore(String)}
 * </p>
 *
 * @api_public
 */
public class DatabaseOpened extends DatabaseModified {

    /**
     * Event for database opened.
     *
     * @param dbName
     *            The name of the datastore that was opened
     */
    public DatabaseOpened(String dbName) {
        super(dbName);
    }

}
