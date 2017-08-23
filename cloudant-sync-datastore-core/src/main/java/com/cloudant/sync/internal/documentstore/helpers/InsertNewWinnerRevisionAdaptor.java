/*
 * Copyright © 2017 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.documentstore.helpers;

import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.documentstore.callables.InsertRevisionCallable;

/**
 * Created by tomblench on 23/08/2017.
 */

public class InsertNewWinnerRevisionAdaptor {

    public static InsertRevisionCallable insert(DocumentBody newWinner,
                                                InternalDocumentRevision oldWinner)
            throws AttachmentException, DocumentStoreException {

        String newRevisionId = CouchUtils.generateNextRevisionId(oldWinner.getRevision());

        InsertRevisionCallable callable = new InsertRevisionCallable();

        callable.docNumericId = oldWinner.getInternalNumericId();
        callable.revId = newRevisionId;
        callable.parentSequence = oldWinner.getSequence();
        callable.deleted = false;
        callable.current = true;
        callable.data = newWinner.asBytes();
        callable.available = true;

        return callable;
    }

}
