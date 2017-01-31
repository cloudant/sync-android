/*
 * Copyright © 2015 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.mazha;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by tomblench on 19/11/15.
 */

/*
 * For deserialising the response from the _bulk_get endpoint
 */

public class BulkGetResponse {

    @JsonProperty
    public List<Result> results;

    static public class Result {

        @JsonProperty
        public String id;

        @JsonProperty
        public List<Doc> docs;
    }

    static public class Doc {

        @JsonProperty
        public DocumentRevs ok;
    }
}
