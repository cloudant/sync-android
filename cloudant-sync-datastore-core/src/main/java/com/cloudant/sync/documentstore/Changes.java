/*
 * Copyright © 2017 IBM Corp. All rights reserved.
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

import java.util.List;

public interface Changes {
    /**
     * <p>Returns the last sequence number of this change set.</p>
     *
     * <p>This number isn't necessarily the same as the sequence number of the
     * last {@code DocumentRevision} in the list of changes.</p>
     *
     * @return last sequence number of the changes set.
     */
    long getLastSequence();

    /**
     * <p>Returns the list of {@code DocumentRevision}s in this change set.</p>
     *
     * @return the list of {@code DocumentRevision}s in this change set.
     */
    List<DocumentRevision> getResults();
}
