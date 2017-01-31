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
 * Thrown when there is a logic or programmer error reading, creating, updating, or deleting
 * {@link DocumentRevision}s or their associated {@link Attachment}s
 * </p>
 * <p>
 * Note: these error cases are distinct from {@link DocumentStoreException}s which are thrown
 * when an unexpected condition was encountered (for example, an internal SQLite database error).
 * </p>
 * <p>
 * This is the base class of a hierarchy of exceptions. In most cases a more specific
 * exception will be thrown. See the documentation for each subclass for specific details.
 * </p>
 *
 * @see DocumentStoreException
 */

public class DocumentException extends Exception {

    public DocumentException(String message) {
        super(message);
    }

    public DocumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public DocumentException(Throwable cause) {
        super(cause);
    }

}
