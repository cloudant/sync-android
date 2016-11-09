/*
 * Copyright (c) 2015 IBM Corp. All rights reserved.
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

import java.util.List;

/**
 * Created by tomblench on 14/10/15.
 */

/**
 * Represents a bulk GET request for documents
 *
 * This is in the format which the _bulk_get endpoint understands, where &lt;doc id, rev id&gt;
 * pairs are given. If multiple rev ids are required, then the doc id needs to be repeated across
 * multiple requests.
 *
 * @api_private
 */
public class BulkGetRequest {
    public String id;
    public String rev;
    public List<String> atts_since;
}
