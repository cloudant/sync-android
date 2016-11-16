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

package com.cloudant.sync.documentstore;

import java.util.Map;

/**
 * Interface for DocumentRevisionTree body. It is logically a JSON document, and physically stored as a blob underneath.
 *
 * <p>{@code DocumentBody} performs a simple serialization/de-serialization between
 * {@code Map<String, Object>} and {@code byte[]}.</p>
 *
 * <ol>
 *   <li>Text literals are de-serialised as {@code java.lang.String}.</li>
 *   <li>Integer number are converted to {@code Integer} or {@code Long}if it
 *   is bigger than {@code Integer.MAX_VALUE} or smaller than
 *   {@code Integer.MIN_VALUE}.</li>
 *   <li>Decimal number are de-serialised as {@code Double}.</li>
 * </ol>
 *
 * <p>If there is any exception with serialization/de-serialization, the entire
 * data will be ignored, and an empty DocumentBody will be created.</p>
 *
 * <p>Field names starting with {@code _} are reserved for internal use, and
 * must not be modified by client code.</p>
 *
 * <p>Changing the data stored within this object will not result in changes
 * to the database unless explicitly saved as a new revision.</p>
 *
 * @api_public
 */
public interface DocumentBody {

    /**
     * <p>Returns a shallow copy of the underlying data as a map.</p>
     *
     * <p>Modifying the map below the first level will be visible to other
     * users of the {@code DocumentBody} object and so should be avoided.</p>
     *
     * @return shallow copy of the data as a {@code Map}.
     */
    public Map<String, Object> asMap();

    /**
     * <p>Returns copy of the data as a byte array.</p>
     *
     * <p>Modifying this data will not affect the data for the DocumentBody.</p>
     *
     * @return copy of the data as byte array
     */
    public byte[] asBytes();

}
