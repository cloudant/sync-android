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
 * <p>Describes the configuration for a replication with the local datastore
 * as target.</p>
 */
class PullConfiguration {

    public static final int DEFAULT_CHANGES_LIMIT_PER_BATCH = 1000;
    public static final int DEFAULT_MAX_BATCH_COUNTER_PER_RUN = 100;
    public static final int DEFAULT_INSERT_BATCH_SIZE = 10;
    public static final boolean DEFAULT_PULL_ATTACHMENTS_INLINE = false;

    final int changeLimitPerBatch;
    final int batchLimitPerRun;
    final int insertBatchSize;
    final boolean pullAttachmentsInline;

    /**
     * <p>Construct a {@code PullConfiguration} with the default settings.</p>
     */
    public PullConfiguration() {
        this(DEFAULT_CHANGES_LIMIT_PER_BATCH, DEFAULT_MAX_BATCH_COUNTER_PER_RUN, DEFAULT_INSERT_BATCH_SIZE, DEFAULT_PULL_ATTACHMENTS_INLINE);
    }

    /**
     * <p>Construct a {@code PullConfiguration} with custom settings.</p>
     * @param changeLimitPerBatch  {@code limit} on {@code _changes} calls.
     * @param batchLimitPerRun The maximum number of batches of changes pulled
     *             from the remote datastore. So the most changes
     *             pulled is {@code changeLimitPerBatch * batchLimitPerRun}.
     *             Its intended use is to stop replications running for ever.
     * @param insertBatchSize Number of changes inserted into local datastore
     *                        at a time.
     */
    public PullConfiguration(int changeLimitPerBatch, int batchLimitPerRun, int insertBatchSize, boolean pullAttachmentsInline) {
        this.changeLimitPerBatch = changeLimitPerBatch;
        this.batchLimitPerRun = batchLimitPerRun;
        this.insertBatchSize = insertBatchSize;
        this.pullAttachmentsInline = pullAttachmentsInline;
    }
}
