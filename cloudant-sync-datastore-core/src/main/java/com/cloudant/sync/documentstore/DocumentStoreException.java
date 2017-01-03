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

package com.cloudant.sync.documentstore;

/**
 * Created by Rhys Short on 05/02/15.
 */

/**
 * <p>
 * Thrown when an unexpected condition was encountered during a {@link DocumentStore} method, for
 * example an internal SQLite database error.
 * </p>
 *
 * @see DocumentException
 * @api_public
 */

public class DocumentStoreException extends Exception {

    public DocumentStoreException(String message){
        super(message);
    }

    public DocumentStoreException(Throwable causedBy){
        super(causedBy);
    }

    public DocumentStoreException(String message, Throwable causedBy){
        super(message,causedBy);
    }

}
