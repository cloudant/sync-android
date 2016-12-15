/*
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.event.notifications;

import com.cloudant.sync.replication.Replicator;

/**
 * <p>Event posted when a state transition to ERROR is completed.</p>
 *
 * <p>Errors may include things such as:</p>
 *
 * <ul>
 *      <li>incorrect credentials</li>
 *      <li>network connection unavailable</li>
 * </ul>
 *
 * @api_public
 */
public class ReplicationErrored implements Notification {

    public ReplicationErrored(Replicator replicator, Throwable errorInfo) {
        this.replicator = replicator;
        this.errorInfo = errorInfo;
    }

    /** 
     * The {@code Replicator} issuing the event
     */
    public final Replicator replicator;

    /** 
     * Error information about the error that occurred
     */
    public final Throwable errorInfo;
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof ReplicationErrored) {
            ReplicationErrored re = (ReplicationErrored)other;
            return this.replicator == re.replicator && this.errorInfo == re.errorInfo;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = replicator != null ? replicator.hashCode() : 0;
        result = 31 * result + (errorInfo != null ? errorInfo.hashCode() : 0);
        return result;
    }
}
