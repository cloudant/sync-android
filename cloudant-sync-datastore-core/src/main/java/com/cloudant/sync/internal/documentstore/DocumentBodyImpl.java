/*
 * Copyright © 2016 IBM Corp. All rights reserved.
 *
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright © 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright © 2013 Cloudant, Inc.
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.internal.util.Misc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public final class DocumentBodyImpl implements DocumentBody {

    private byte[] bytes;
    private Map<String, Object> map;

    public DocumentBodyImpl(byte[] bytes) {
        // compacted revisions have their bodies set to null, so return an empty body
        if (bytes == null) {
            bytes = JSONUtils.emptyJSONObjectAsBytes();
        }
        if(JSONUtils.isValidJSON(bytes)) {
            this.bytes = bytes;
        } else {
            throw new IllegalArgumentException("Input bytes is not valid json data.");
        }
    }

    public DocumentBodyImpl(Map map) {
        // Note uses checkArgument not checkNotNull to keep IllegalArgumentException not NPE
        Misc.checkArgument(map != null, "Document body map must not be null.");
        if(JSONUtils.isValidJSON(map)) {
            this.map = map;
        } else {
            throw new IllegalArgumentException("Input map is not valid json data.");
        }
    }

    public static DocumentBody bodyWith(byte[] bytes) {
        return new DocumentBodyImpl(bytes);
    }

    public static DocumentBody bodyWith(Map map) {
        return new DocumentBodyImpl(map);
    }

    @Override
    public byte[] asBytes() {
        byte[] jsonCopy = getJsonBytes();
        return Arrays.copyOf(jsonCopy, jsonCopy.length);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> asMap() {
        return (Map<String,Object>) getMapObject();
    }

    @Override
    public String toString() {
        if(bytes != null) {
            return JSONUtils.bytesToString(bytes);
        } else if(map != null) {
            return JSONUtils.serializeAsString(map);
        } else {
            throw new IllegalStateException();
        }
    }

    private byte[] getJsonBytes() {
        if(bytes == null) {
            assert map != null;
            bytes = JSONUtils.serializeAsBytes(map);
        }
        return bytes.clone();
    }

    private Map getMapObject() {
        if(map == null) {
            assert bytes != null;
            map = JSONUtils.deserialize(bytes);
        }

        // Return a shallow copy
        return new HashMap(map);
    }
}
