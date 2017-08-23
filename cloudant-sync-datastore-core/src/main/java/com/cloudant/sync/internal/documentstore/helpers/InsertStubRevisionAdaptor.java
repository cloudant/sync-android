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

import com.cloudant.sync.internal.documentstore.callables.InsertRevisionCallable;
import com.cloudant.sync.internal.util.JSONUtils;

/**
 * Created by tomblench on 23/08/2017.
 */

public class InsertStubRevisionAdaptor {

    public static InsertRevisionCallable insert(long docNumericId, String revId, long
            parentSequence) {
        // don't copy attachments
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = docNumericId;
        callable.revId = revId;
        callable.parentSequence = parentSequence;
        callable.deleted = false;
        callable.current = false;
        callable.data = JSONUtils.emptyJSONObjectAsBytes();
        callable.available = false;
        return callable;
    }

}
