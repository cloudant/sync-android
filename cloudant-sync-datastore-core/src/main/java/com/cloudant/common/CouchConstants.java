/**
 * Copyright (C) 2013 Cloudant
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

package com.cloudant.common;

/**
 * @api_private
 */
public class CouchConstants {

    // Special fields are from: http://wiki.apache.org/couchdb/HTTP_Document_API#Special_Fields
    public static final String _id = "_id";
    public static final String _rev = "_rev";

    public static final String _conflicts = "_conflicts";
    public static final String _revisions = "_revisions";
    public static final String _deleted = "_deleted";

    public static final String _attachments = "_attachments";
    public static final String _revs_info = "_revs_info";
    public static final String _deleted_conflicts = "_deleted_conflicts";
    public static final String _local_seq = "_local_seq";

    public static final String start = "start";
    public static final String ids = "ids";

    public static final String _design_prefix = "_design/";
    public static final String _design_prefix_encoded = "_design%2F";
    public static final String _local_prefix = "_local/";
    public static final String _local_prefix_encoded = "_local%2F";

}
