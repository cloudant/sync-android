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

package com.cloudant.sync.internal.util;

import com.cloudant.sync.internal.common.PropertyFilterMixIn;
import com.cloudant.sync.internal.mazha.Document;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Internal utility class
 *
 */
public class JacksonModule extends SimpleModule {

    private static final long serialVersionUID = 5112015974963951685L;

    @SuppressWarnings("deprecation")
    public JacksonModule() {
         super("JacksonModule", new Version(0,0,1,null));
    }

    @Override
    public void setupModule(SetupContext context){
        // important, it takes advantage of mixin to use filter to filter out couchdb keywords for
        // all the serialization, notice is only works for sub-class of DocumentRevisionTree
        context.setMixInAnnotations(Document.class, PropertyFilterMixIn.class);
    }
}
