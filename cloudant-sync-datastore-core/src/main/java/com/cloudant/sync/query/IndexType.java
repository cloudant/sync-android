/*
 *  Copyright (c) 2016 IBM Corp. All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *   License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 */

package com.cloudant.sync.query;

/**
 * Denotes the type for an Query Index.
 */
public enum IndexType {

    /**
     * JSON Index
     */
    JSON,
    /**
     * Text Index
     */
    TEXT;

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

    /**
     * Converts a string to its Enum value.
     * @param value The string to convert to an enum
     * @return The enum value for the String or {@code null}
     */
    public static IndexType enumValue(String value){
        if(value.equals(IndexType.JSON.toString())){
            return IndexType.JSON;
        } else if (value.equals(IndexType.TEXT.toString())){
            return IndexType.TEXT;
        }
        return null;
    }

}
