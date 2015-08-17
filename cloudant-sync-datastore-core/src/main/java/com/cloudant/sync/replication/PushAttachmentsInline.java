/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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
 * <p>
 * Strategy to decide whether to push attachments inline:
 * </p>
 *
 * <ul>
 *     <li>False: Always push attachments separately as an HTTP binary PUT</li>
 *     <li>Small: Push small attachments inline, and large attachments separately.
 *         Uses SavedAttachment.isLarge() to determine whether attachment is small or large.</li>
 *     <li>True: Always push attachments inline as a base64-encoded string.</li>
 * </ul>
 *
 */

public enum PushAttachmentsInline {
    False,
    Small,
    True
}
