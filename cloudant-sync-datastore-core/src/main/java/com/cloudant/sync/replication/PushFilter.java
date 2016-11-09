/*
 *  Copyright (C) 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.replication;

import com.cloudant.sync.documentstore.DocumentRevision;
/*
 * Created by Rhys Short on 31/03/2016.
 */

/**
 * <P>
 * Interface for filtering documents during a push replication.
 * </P>
 * <P>
 * Example usage - a filter for replicating only documents with an ID beginning with the letter a:
 * </P>
 * <pre>
 * <code>
 * replicatorBuilder.filter(new PushFilter(){
 *             {@literal @}Override
 *              public boolean shouldReplicateDocument(DocumentRevision revision) {
 *                  if (revision.getId().startsWith("a") {
 *                      return true;
 *                  } else {
 *                      return false;
 *                  }
 *              }
 *     });
 * </code>
 * </pre>
 *
 * @api_public
 * @see com.cloudant.sync.replication.ReplicatorBuilder.Push#filter(PushFilter)
 */
public interface PushFilter {

    /**
     * Determines if a DocumentRevision should be replicated to the remote database
     *
     * @param revision the DocumentRevision under consideration for replication
     * @return true if the DocumentRevision should be replicated.
     */
    boolean shouldReplicateDocument(DocumentRevision revision);

}
