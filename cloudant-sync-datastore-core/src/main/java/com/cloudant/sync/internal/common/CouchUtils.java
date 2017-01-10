/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.common;

import com.cloudant.sync.internal.util.Misc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class CouchUtils {

    public static String createUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static void validateDocumentId(String docId) {
        if (!isValidDocumentId(docId)) {
            throw new IllegalArgumentException("DBObject id is not in right format: " + docId);
        }
    }

    public static boolean isValidDocumentId(String docId) {
        // http://wiki.apache.org/couch/HTTP_Document_API#Documents
        if (Misc.isStringNullOrEmpty(docId)) {
            return false;
        }

        if (docId.charAt(0) == '_') {
            return (docId.startsWith(CouchConstants._design_prefix)
                    || docId.startsWith(CouchConstants._local_prefix));
        }

        return true;
    }

    /**
     * Get an ascending order revision list from the couch style {@code _revisions} history of
     * {@code start} and hash {@code ids}.
     *
     * @param start the starting generation
     * @param ids   the list of ID hashes
     * @return
     */
    public static List<String> couchStyleRevisionHistoryToFullRevisionIDs(final int start, List<String> ids) {
        List<String> revisionHistory = new ArrayList<String>();
        int generation = start;
        for (String revIdHash : ids) {
            revisionHistory.add(generation + "-" + revIdHash);
            generation--;
        }
        Collections.reverse(revisionHistory);
        return revisionHistory;
    }

    public static String getFirstLocalDocRevisionId() {
        return Integer.toString(1) + "-local";
    }

    public static String getFirstRevisionId() {
        String digest = createUUID();
        return Integer.toString(1) + "-" + digest;
    }

    public static String generateNextLocalRevisionId(String revisionId) {
        int generation = generationFromRevId(revisionId);
        return String.valueOf(generation + 1) + "-local";
    }

    // DBObject IDs have a generation count, a hyphen, and a UUID.
    public static String generateNextRevisionId(String revisionId) {
        validateRevisionId(revisionId);
        int generation = generationFromRevId(revisionId);
        String digest = createUUID();
        return Integer.toString(generation + 1) + "-" + digest;
    }

    public static int generationFromRevId(String revID) {
        validateRevisionId(revID);
        return Integer.parseInt(revID.substring(0, revID.indexOf("-")));
    }

    public static boolean isValidRevisionId(String revisionId) {
        if (Misc.isStringNullOrEmpty(revisionId)) {
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
            throw new IllegalArgumentException("DBObject revision id is not in right format: " + revisionId);
        }
    }

    public static String generateDocumentId() {
        return createUUID();
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

    public static String[] getRevisionIdsSuffix(String... revIds) {
        if(revIds == null || revIds.length == 0) {
            return new String[0];
        }
        String[] res = new String[revIds.length];
        for(int i = 0 ; i < revIds.length ; i ++) {
            res[i] = getRevisionIdSuffix(revIds[i]);
        }
        return res;
    }
}
