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

package com.cloudant.sync.internal.mazha.json;

import com.cloudant.sync.internal.mazha.MissingOpenRevision;
import com.cloudant.sync.internal.mazha.OkOpenRevision;
import com.cloudant.sync.internal.mazha.OpenRevision;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class OpenRevisionDeserializer extends JsonDeserializer<OpenRevision> {

    @Override
    public OpenRevision deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        ObjectNode node = jp.readValueAsTree();
        if(node.has("ok")) {
            return jp.getCodec().treeToValue(node, OkOpenRevision.class);
        } else if(node.has("missing")) {
            return jp.getCodec().treeToValue(node, MissingOpenRevision.class);
        } else {
            // Should never happen
            throw new IllegalStateException("Unexpected object in open revisions response.");
        }
    }
}
