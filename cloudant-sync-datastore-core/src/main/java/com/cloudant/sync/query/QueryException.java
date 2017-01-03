/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.query;

import com.cloudant.sync.documentstore.DocumentStoreException;

/**
 * <p>
 * Thrown when an unexpected condition was encountered during a {@link Query} method, for
 * example an internal SQLite database error.
 * </p>

 * @api_public
 */
public class QueryException extends DocumentStoreException {
    public QueryException(Throwable causedBy){
        super(causedBy);
    }

    public QueryException(String message){
        super(message);
    }

    public QueryException(String message, Throwable causedBy) {
        super(message, causedBy);
    }

}
