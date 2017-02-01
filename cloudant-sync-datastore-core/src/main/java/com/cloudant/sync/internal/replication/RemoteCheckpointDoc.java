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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.internal.mazha.Document;

class RemoteCheckpointDoc extends Document {

    private String lastSequence;

    // Default constructor is required by Jackson
    public RemoteCheckpointDoc() {} ;

    public RemoteCheckpointDoc(String lastSequence) {
        this.lastSequence = lastSequence;
    }

    public String getLastSequence() {
        return lastSequence;
    }

    public void setLastSequence(String lastSequence) {
        this.lastSequence = lastSequence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        RemoteCheckpointDoc that = (RemoteCheckpointDoc) o;

        return lastSequence != null ? lastSequence.equals(that.lastSequence) : that.lastSequence
                == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (lastSequence != null ? lastSequence.hashCode() : 0);
        return result;
    }
}
