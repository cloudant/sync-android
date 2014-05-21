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
 *     </li>True: Always push attachments inline as a base64-encoded string.</li>
 * </ul>
 *
 * @see com.cloudant.sync.datastore.SavedAttachment#isLarge()
 */

public enum PushAttachmentsInline {
    False,
    Small,
    True
}
