/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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

package com.cloudant.sync.internal.documentstore;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NotificationTestUtils {

    static int timeoutSecs = 2;

    static boolean waitForSignal(CountDownLatch b) {
        try {
            return b.await(timeoutSecs, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            return false;
        }
    }

}
