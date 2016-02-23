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

import com.cloudant.sync.notifications.ReplicationCompleted;
import com.cloudant.sync.notifications.ReplicationErrored;
import com.google.common.eventbus.Subscribe;

public class TestReplicationListener {

    public boolean errorCalled = false;
    public boolean finishCalled = false;
    public Throwable exception = null;
    public int batches = 0;
    public int docs = 0;

    @Subscribe
    public void complete(ReplicationCompleted rc) {
        finishCalled = true;
        batches = rc.batchesReplicated;
        docs = rc.documentsReplicated;
    }

    @Subscribe
    public void error(ReplicationErrored re) {
        errorCalled = true;
        exception = re.errorInfo.getException();
    }
}
