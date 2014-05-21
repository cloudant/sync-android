/**
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright (c) 2013 Cloudant, Inc.
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

package com.cloudant.sync.datastore;

import com.cloudant.sync.util.JSONUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

final class BasicDocumentBody implements DocumentBody {

    private byte[] bytes;
    private Map<String, Object> map;

    protected BasicDocumentBody(byte[] bytes) {
        assert bytes != null;
        if(JSONUtils.isValidJSON(bytes)) {
            this.bytes = bytes;
        } else {
            throw new IllegalArgumentException("Input bytes is not valid json data.");
        }
    }

    protected BasicDocumentBody(Map map) {
        assert map != null;
        if(JSONUtils.isValidJSON(map)) {
            this.map = map;
        } else {
            throw new IllegalArgumentException("Input map is not valid json data.");
        }
    }

    public static DocumentBody bodyWith(byte[] bytes) {
        return new BasicDocumentBody(bytes);
    }

    public static DocumentBody bodyWith(Map map) {
        return new BasicDocumentBody(map);
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
