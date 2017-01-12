/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant
 *
 * Copyright © 2011 Ahmed Yehia (ahmed.yehia.m@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudant.sync.internal.mazha;

import com.cloudant.sync.internal.util.Misc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Convenience class to access the response of GET document request with open revision options. An example response
 * is following:
 * </p>
 * <pre>
 * {@code 
 * [{
 *     "ok" : {
 *         "_id" : "c3fe5bfdee767fa3d51717bb8b2a9349",
 *         "_rev" : "2-7024cbfb49a24791979476148196e669",
 *         "hello" : "world",
 *         "name" : "Alex",
 *         "_revisions" : {
 *             "start" : 2,
 *             "ids" : [
 *                 "7024cbfb49a24791979476148196e669",
 *                 "15f65339921e497348be384867bb940f" ]
 *         }
 *     }
 * },
 * {
 *     "ok" : {
 *         "_id" : "c3fe5bfdee767fa3d51717bb8b2a9349",
 *         "_rev" : "2-a63239adb4844666a48e070b64c1f965",
 *         "hello" : "world",
 *         "name" : "Jerry",
 *         "_revisions" : {
 *             "start" : 2,
 *             "ids" : [
 *                 "a63239adb4844666a48e070b64c1f965",
 *                 "15f65339921e497348be384867bb940f" ]
 *         }
 *     }
 * },
 * {
 *     "missing" : "3-2fea9fd0ed9c4f68b394b40b8310fc14bad"
 * },
 * {
 *     "missing" : "2-a63239adb4844666a48e070b64c1f965bad"
 * }]
 * }
 * </pre>
 *
 * The response firstly is convert to a <code>List</code> of <code>Map</code>, which could be used to construct
 * and <code>GetOpenRevisionsResponse</code> object.
 *
 * @api_private
 */

public class GetOpenRevisionsResponse {

    // From open rev to OkOpenRevision
    private final Map<String, OkOpenRevision> okDataMap = new HashMap<String, OkOpenRevision>();

    // From open rev to MissingOpenRevision
    private final Map<String, MissingOpenRevision> missingDataMap = new HashMap<String, MissingOpenRevision>();

    public GetOpenRevisionsResponse(List<OpenRevision> data) {
        Misc.checkNotNull(data, "Data");

        for(OpenRevision d : data) {
            if(d instanceof OkOpenRevision) {
                OkOpenRevision ok = (OkOpenRevision) d;
                okDataMap.put(ok.getDocumentRevs().getRev(), ok);
            } else if(d instanceof MissingOpenRevision) {
                MissingOpenRevision missing = (MissingOpenRevision)d;
                missingDataMap.put(missing.getRevision(), missing);
            } else {
                // Should never happen
                throw new IllegalStateException("Unexpected data type:" + d.getClass());
            }
        }
    }

    public Map<String, OkOpenRevision> getOkRevisionMap() {
        return okDataMap;
    }

    public Map<String, MissingOpenRevision> getMissingRevisionsMap() {
        return missingDataMap;
    }

    public List<String> findRevisionsIdForOpenRev(String openRev) {
        return okDataMap.get(openRev).getDocumentRevs().getRevisions().getIds();
    }
}
