/*
 * Copyright © 2015 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.documentstore.migrations;

import com.cloudant.sync.internal.sqlite.SQLDatabase;

/**
 * Interface defining methods for running migration.
 */
public interface Migration {

    /**
     * Implementors should run all migration steps in this method.
     *
     * Throw an exception if the migration fails.
     *
     * @param db The {@link SQLDatabase} to migrate
     * @throws Exception an exception was thrown during migration
     */
    void runMigration(SQLDatabase db) throws Exception;

}
