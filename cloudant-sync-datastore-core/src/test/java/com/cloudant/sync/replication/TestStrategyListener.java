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

import com.cloudant.sync.event.Subscribe;

/**
 * Simple implementation of <code>StrategyListener</code>. It can be checked if the complete() or
 * error() has been called or not.
 */

public class TestStrategyListener {

    public boolean errorCalled = false;
    public boolean finishCalled = false;
    public int documentsReplicated = 0;
    public int batchesReplicated = 0;

    @Subscribe
    public void complete(ReplicationStrategyCompleted rc) {
        finishCalled = true;
        documentsReplicated = rc.replicationStrategy.getDocumentCounter();
        batchesReplicated = rc.replicationStrategy.getBatchCounter();
    }

    @Subscribe
    public void error(ReplicationStrategyErrored re) {
        errorCalled = true;
    }
}
