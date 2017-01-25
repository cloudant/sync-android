/*
 * Copyright © 2015 IBM Corp. All rights reserved.
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

package com.cloudant.sync.matcher;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * <p>Matcher for use in ExpectedException#expectCause()</p>
 *
 * <p>Usage:</p>
 *
 * <pre>
 * exception.expectCause(new CauseMatcher(IllegalBlockSizeException.class));
 * </pre>
 */
public class CauseMatcher extends TypeSafeMatcher<Throwable> {

    private final Class<? extends Throwable> type;

    public CauseMatcher(Class<? extends Throwable> type) {
        this.type = type;
    }

    @Override
    protected boolean matchesSafely(Throwable item) {
        return item.getClass().isAssignableFrom(type);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("expects type ").appendValue(type);
    }
}
