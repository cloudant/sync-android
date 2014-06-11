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
 * as source.</p>
 */
class PushConfiguration {

    public static final int DEFAULT_CHANGES_LIMIT_PER_BATCH = 500;
    public static final int DEFAULT_MAX_BATCH_COUNTER_PER_RUN = 100;
    public static final int DEFAULT_BULK_INSERT_SIZE = 10;
    // by default push small attachments as inline base64, and larger ones as multipart
    public static final PushAttachmentsInline DEFAULT_PUSH_ATTACHMENTS_INLINE = PushAttachmentsInline.Small;

    final int changeLimitPerBatch;
    final int batchLimitPerRun;
    final int bulkInsertSize;
    final PushAttachmentsInline pushAttachmentsInline;

    /**
     * <p>Construct a {@code PushConfiguration} with the default settings.</p>
     */
    public PushConfiguration() {
        this(DEFAULT_CHANGES_LIMIT_PER_BATCH, DEFAULT_MAX_BATCH_COUNTER_PER_RUN, DEFAULT_BULK_INSERT_SIZE, DEFAULT_PUSH_ATTACHMENTS_INLINE);

    }

    /**
     * <p>Construct a {@code PullConfiguration} with custom settings.</p>
     * @param changeLimitPerBatch Number of changes pulled from local datastore
     *                            at a time.
     * @param batchLimitPerRun The maximum number of batches of changes pulled
     *             from the local datastore. So the most changes
     *             pushed is {@code changeLimitPerBatch * batchLimitPerRun}.
     *             Its intended use is to stop replications running for ever.
     * @param insertBatchSize Number of changes inserted into remote datastore
     *                        at a time.
     * @param pushAttachmentsInline Strategy to decide whether to push attachment
     *                              inline or separately.
     */
    public PushConfiguration(int changeLimitPerBatch, int batchLimitPerRun, int insertBatchSize, PushAttachmentsInline pushAttachmentsInline) {
        this.changeLimitPerBatch = changeLimitPerBatch;
        this.batchLimitPerRun = batchLimitPerRun;
        this.bulkInsertSize = insertBatchSize;
        this.pushAttachmentsInline = pushAttachmentsInline;
    }
}
