/*
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

package com.cloudant.sync.documentstore;

import com.cloudant.sync.internal.datastore.DocumentBodyImpl;
import com.cloudant.sync.internal.util.JSONUtils;

import java.util.Map;

/**
 * <p>Factory class to create DocumentBody objects from either JSON byte-streams
 * or java Map objects.</p>
 *
 * @api_public
 */
public class DocumentBodyFactory {

    /**
     * <p>Returns a {@link DocumentBody}  object representing an empty document body.
     * This instance is shared.</p>
     */
    public final static DocumentBodyImpl EMPTY = new DocumentBodyImpl(JSONUtils.emptyJSONObjectAsBytes());

    /**
     * <p>Returns a new {@link DocumentBody} object from JSON data.</p>
     * @param bytes JSON data
     * @return DocumentBody object containing given data.
     */
    public static DocumentBody create(byte[] bytes) {
        return new DocumentBodyImpl(bytes);
    }

    /**
     * <p>Returns a new {@link DocumentBody} object from Map serializable as JSON.</p>
     * @param map JSON data as map
     * @return DocumentBody object containing given data.
     */
    public static DocumentBody create(Map map) {
        return new DocumentBodyImpl(map);
    }
}
