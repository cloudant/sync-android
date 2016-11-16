/*
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

package com.cloudant.sync.internal.mazha.matcher;

import com.cloudant.sync.internal.util.Misc;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class IsNotEmpty extends TypeSafeMatcher<String> {

    @Override
    protected boolean matchesSafely(String item) {
        return !Misc.isStringNullOrEmpty(item);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("not empty");
    }

    @Factory()
    public static Matcher<String> notEmpty() {
        return new IsNotEmpty();
    }
}
