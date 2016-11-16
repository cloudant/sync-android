/*
 * Copyright © 2015, 2016 IBM Corp. All rights reserved.
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

import com.cloudant.sync.internal.documentstore.RevisionHistoryHelper;

import java.util.List;

/**
 * <p>
 * Strategy to decide whether to push attachments inline:
 * </p>
 *
 * <ul>
 *     <li>False: Always push attachments separately from JSON body, via multipart/related</li>
 *     <li>Small: Push small attachments inline, and large attachments separately.
 *         Uses SavedAttachment.isLarge() to determine whether attachment is small or large.</li>
 *     <li>True: Always push attachments inline in JSON body, as a base64-encoded string.</li>
 * </ul>
 *
 * <p>
 * Note that all attachments belonging to a pushed revision are either sent via multipart/related,
 * or all inline in JSON body, as a base64-encoded string.
 * Further details of this can be found at
 * {@link RevisionHistoryHelper#shouldInline(List, PushAttachmentsInline, int)}
 * </p>
 *
 * @see RevisionHistoryHelper
 *
 * @api_public
 */

public enum PushAttachmentsInline {
    False,
    Small,
    True
}
