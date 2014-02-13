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

class BasicIndex implements Index, Comparable<BasicIndex> {

    private final String indexName;
    private Long lastSequence;
    private final IndexType fieldType;

    public BasicIndex(String name, IndexType type, Long lastSequence) {
        this.indexName = name;
        this.fieldType = type;
        this.lastSequence = lastSequence;
    }

    public BasicIndex(String name, IndexType type) {
        this(name, type, -1l);
    }

    public BasicIndex(String name) {
        this(name, IndexType.STRING, -1l);
    }

    @Override
    public String getName() {
        return this.indexName;
    }

    @Override
    public Long getLastSequence() {
        return this.lastSequence;
    }

    public void setLastSequence(Long sequence) {
        this.lastSequence  = sequence;
    }

    @Override
    public IndexType getIndexType() {
        return this.fieldType;
    }

    @Override
    public int hashCode() {
        int h = this.indexName.hashCode();
        h += h*31 + fieldType.hashCode();
        return  h;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof BasicIndex)) {
            return false;
        }

        BasicIndex that = (BasicIndex)o;
        return equalTo(that.getName(), that.getIndexType());
    }

    public boolean equalTo(String indexName, IndexType type) {
        return this.indexName.equals(indexName)
                && this.fieldType.equals(type);
    }

    @Override
    public int compareTo(BasicIndex o) {
        return this.indexName.compareTo(o.getName());
    }
}
