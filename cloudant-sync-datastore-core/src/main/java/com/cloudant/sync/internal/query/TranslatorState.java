//  Copyright (c) 2014 Cloudant. All rights reserved.
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

package com.cloudant.sync.internal.query;

/**
 *  The purpose of a TranslatorState object is to track the state of a query translation operation
 *  performed by method calls in the {@link QuerySqlTranslator}.  Since
 *  all methods within the {@link QuerySqlTranslator} class that
 *  utilize a TranslatorState object are static in nature, the TranslatorState class cannot be
 *  implemented as an inner class to {@link QuerySqlTranslator} and must be
 *  implemented this way instead.
 *
 *  @see QuerySqlTranslator
 */
class TranslatorState {

    public boolean atLeastOneIndexUsed;
    public boolean atLeastOneIndexMissing;
    public boolean atLeastOneORIndexMissing;
    public boolean textIndexRequired;
    public boolean textIndexMissing;

    TranslatorState() {
        atLeastOneIndexUsed = false;
        atLeastOneIndexMissing = false;
        atLeastOneORIndexMissing = false;
        textIndexRequired = false;
        textIndexMissing = false;
    }

}
