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

package com.cloudant.sync.util;

import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;

/**
 * Internal utility class
 * @api_private
 */
public class CouchUtils {

    public static void validateDocumentId(String docId) {
        if (!isValidDocumentId(docId)) {
            throw new IllegalArgumentException("DocumentRevision id is not in right format: " + docId);
        }
    }

    public static boolean isValidDocumentId(String docId) {
        // http://wiki.apache.org/couch/HTTP_Document_API#Documents
        if (Strings.isNullOrEmpty(docId)) {
            return false;
        }

        if (docId.charAt(0) == '_') {
            return (docId.startsWith("_design/") || docId.startsWith("_local/"));
        }

        return true;
    }

    /*
     * Parses the _revisions dict from a document into an array of revision ID strings
     */
    public static List<String> parseCouchDBRevisionHistory(Map<String, Object> docProperties) {
        Map<String, Object> revisions = (Map<String, Object>) docProperties.get("_revisions");
        if (revisions == null) {
            return null;
        }
        List<String> revIDs = (List<String>) revisions.get("ids");
        Integer start = (Integer) revisions.get("start");
        if (start != null) {
            for (int i = 0; i < revIDs.size(); i++) {
                String revID = revIDs.get(i);
                revIDs.set(i, Integer.toString(start--) + "-" + revID);
            }
        }
        return revIDs;
    }

    public static String getFirstLocalDocRevisionId() {
        return Integer.toString(1) + "-local";
    }

    public static String getFirstRevisionId() {
        String digest = Misc.createUUID();
        return Integer.toString(1) + "-" + digest;
    }

    public static String generateNextLocalRevisionId(String revisionId) {
        int generation = CouchUtils.generationFromRevId(revisionId);
        return String.valueOf(generation + 1) + "-local";
    }

    // DocumentRevision IDs have a generation count, a hyphen, and a UUID.
    public static String generateNextRevisionId(String revisionId) {
        validateRevisionId(revisionId);
        int generation = CouchUtils.generationFromRevId(revisionId);
        String digest = Misc.createUUID();
        return Integer.toString(generation + 1) + "-" + digest;
    }

    public static int generationFromRevId(String revID) {
        validateRevisionId(revID);
        return Integer.parseInt(revID.substring(0, revID.indexOf("-")));
    }

    public static boolean isValidRevisionId(String revisionId) {
        if (Strings.isNullOrEmpty(revisionId)) {
            return false;
        }
        int dashPos = revisionId.indexOf("-");
        if (dashPos < 0) {
            return false;
        } else {
            try {
                Integer.parseInt(revisionId.substring(0, dashPos));
            } catch (NumberFormatException e) {
                return false;
            }
            return true;
        }
    }

    public static void validateRevisionId(String revisionId) {
        if (!isValidRevisionId(revisionId)) {
            throw new IllegalArgumentException("DocumentRevision revision id is not in right format: " + revisionId);
        }
    }

    public static String generateDocumentId() {
        return Misc.createUUID();
    }

    // Splits a revision ID into its generation number and opaque suffix string
    public static String getRevisionIdSuffix(String revId) {
        validateRevisionId(revId);
        int dashPos = revId.indexOf("-");
        if (dashPos >= 0) {
            return revId.substring(dashPos + 1);
        } else {
            throw new IllegalStateException("The revId id should be valid: " + revId);
        }
    }
}
