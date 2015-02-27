//  Copyright (c) 2015 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

final class QueryConstants {

    public static final String AND = "$and";

    public static final String OR = "$or";

    public static final String NOT = "$not";

    public static final String EXISTS = "$exists";

    public static final String EQ = "$eq";

    public static final String NE = "$ne";

    public static final String IN = "$in";

    private QueryConstants() {
        throw new AssertionError();
    }
    
}
