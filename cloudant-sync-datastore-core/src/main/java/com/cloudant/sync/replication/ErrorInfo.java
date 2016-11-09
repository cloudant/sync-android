/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.replication;

/**
 * <p>Describes errors which happen during replication, and usually contains
 * any exception thrown in the {@link ErrorInfo#t} field.</p>
 *
 * @see Replicator
 * @see com.cloudant.sync.event.notifications.ReplicationErrored
 *
 * @api_public
 */
public class ErrorInfo {

    private final Throwable t;

    /**
     * <p>Construct an instance with a {@code Throwable}.</p>
     * @param t {@code Throwable} to construct the instance with.
     */
    public ErrorInfo(Throwable t) {
        this.t = t;
    }

    /**
     * <p>Returns the {@code Throwable} this instance was constructed with.</p>
     * @return the {@code Throwable} this instance was constructed with
     */
    public Throwable getException() {
        return this.t;
    }
}
