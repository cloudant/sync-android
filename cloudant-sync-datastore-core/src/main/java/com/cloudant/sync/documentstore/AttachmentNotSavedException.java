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
 *   Thrown when there is an error saving attachments to local storage.
 * </p>
 * <p>
 *   Likely causes of this exception include:
 * </p>
 * <ul>
 *   <li>IO exception whilst saving attachment to local storage.</li>
 *   <li>Error whilst updating attachment information in the database.</li>
 *   <li>Mismatch between signalled attachment size and actual attachment size.</li>
 * </ul>
 */
public class AttachmentNotSavedException extends AttachmentException {

    public AttachmentNotSavedException(String message) {
        super(message);
    }

    public AttachmentNotSavedException(String message, Throwable cause) {
        super(message, cause);
    }

    public AttachmentNotSavedException(Throwable cause) {
        super(cause);
    }

}
