/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.indexing;

/**
 * <p>
 * Index maps documents from the {@link com.cloudant.sync.datastore.Datastore}
 * to list of values, and then you can query over them efficiently.
 * </p>
 * <p>
 * The mapping is defined using {@link IndexFunction}. {@code IndexFunction}
 * returns a list of {@code Object}. The type of these object must be supported
 * by {@link IndexType}
 * </p>
 * <p>
 * Indexes are managed by {@link IndexManager}.
 * </p>
 *
 * @see IndexManager
 * @see IndexFunction
 * @see IndexType
 * @see com.cloudant.sync.datastore.Datastore
 */
interface Index {

    /**
     * Returns name of the index
     *
     * @return name of the index
     */
    public String getName();

    /**
     * Returns the last sequence of the last document indexed
     *
     * @return Last sequence of the last document indexed.
     */
    public Long getLastSequence();

    /**
     *
     * Returns the type of the index value
     *
     * @return {@link com.cloudant.sync.indexing.IndexType} type of
     *         the index value, aka type of the index.
     *
     * @see IndexType
     */
    public IndexType getIndexType();

    /**
     * Checks if the index's name and type equal to the give indexName
     * and type.
     *
     * @return return true if current index has the same name and type.
     */
    public boolean equalTo(String indexName, IndexType type);

}
