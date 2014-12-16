//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.DocumentBody;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class ValueExtractor {

    private static final Logger logger = Logger.getLogger(ValueExtractor.class.getName());

    public static Object extractValueForFieldName(String possiblyDottedField
                                                , DocumentBody body) {
        // The algorithm here is to split the fields into a "path" and a "lastSegment".
        // The path leads us to the final sub-document. We know that if we have either
        // nil or a non-dictionary object while traversing path that the body doesn't
        // have the right fields for this field selector -- it allows us to make sure
        // that each level of the `path` results in a document rather than a value,
        // because if it's a value, we can't continue the selection process.

        List<String> path = Arrays.asList(possiblyDottedField.split("."));
        String lastSegment = path.remove(path.size() - 1);

        Map<String, Object> currentLevel = body.asMap();
        for (String field: path) {
            Object map = currentLevel.get(field);
            if (map != null && map instanceof Map) {
                currentLevel = (Map<String, Object>) map;
            } else {
                String msg = String.format("Could not extract field %s from document."
                                          , possiblyDottedField);
                logger.log(Level.FINE, msg);
                return null;
            }
        }

        return currentLevel.get(lastSegment);
    }

}
