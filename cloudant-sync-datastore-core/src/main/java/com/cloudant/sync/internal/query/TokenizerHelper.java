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

package com.cloudant.sync.internal.query;

import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.query.Tokenizer;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to help read and write tokenizer setting to and from database
 */

public class TokenizerHelper {

    private static String TOKENIZE = "tokenize";
    private static String TOKENIZE_ARGS = "tokenize_args";

    /**
     * Convert Tokenizer into serialized options, or empty map if null
     * @param tokenizer a {@link Tokenizer} used by a text index, or null
     *                  (this will be the case for JSON indexes)
     * @return a JSON string representing these options
     */
    public static String tokenizerToJson(Tokenizer tokenizer) {
        Map<String, String> settingsMap = new HashMap<String, String>();
        if (tokenizer != null) {
            settingsMap.put(TOKENIZE, tokenizer.tokenizerName);
            // safe to store args even if they are null
            settingsMap.put(TOKENIZE_ARGS, tokenizer.tokenizerArguments);
        }
        return JSONUtils.serializeAsString(settingsMap);
    }

    /**
     * Convert serialized options into Tokenizer, or null if options not present
     * (this will be the case for JSON indexes)
     *
     * @param json Serialized options, as stored in database
     * @return a {@link Tokenizer} representing these options, or null
     */
    public static Tokenizer jsonToTokenizer(String json) {
        if (json != null) {
            Map<String, Object> settingsMap = JSONUtils.deserialize(json.getBytes(Charset.forName
                    ("UTF-8")));

            if (settingsMap.containsKey(TOKENIZE) && settingsMap.get(TOKENIZE) instanceof String) {
                // optional arguments
                String tokenizerArguments = null;
                if (settingsMap.containsKey(TOKENIZE_ARGS) && settingsMap.get(TOKENIZE_ARGS)
                        instanceof String) {
                    tokenizerArguments = (String) settingsMap.get(TOKENIZE_ARGS);
                }
                String tokenizer = (String) settingsMap.get(TOKENIZE);
                return new Tokenizer(tokenizer, tokenizerArguments);
            }
        }
        return null;
    }
}
