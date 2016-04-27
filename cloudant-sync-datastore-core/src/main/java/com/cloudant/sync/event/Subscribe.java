/*
 * Copyright (c) 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.event;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate a method should be subscribed to events posted to the {@link EventBus} on
 * which the method's owning instance is registered.
 * <p>
 * Methods using this annotation must have only a single parameter and must be visible to the
 * EventBus (i.e. the owning class and method itself must be public). The parameter type of the
 * annotated method is of the type of event that notifications will be received for.
 * </p>
 * <p>
 * Subscribers are called synchronously so methods using this annotation must not perform long
 * running operations and should spawn a separate thread if needed.
 * </p>
 * @api_public
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Subscribe {
}
