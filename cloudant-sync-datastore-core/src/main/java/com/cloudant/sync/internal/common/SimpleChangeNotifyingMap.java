/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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
package com.cloudant.sync.internal.common;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by tomblench on 10/02/16.
 *
 * @api_private
 */
public class SimpleChangeNotifyingMap {

    private static List<String> METHODS_OF_CHANGE = Arrays.asList(new String[]{"put", "putAll",
            "remove", "clear"});

    @SuppressWarnings("unchecked")
    public static <K, V> ChangeNotifyingMap<K, V> wrap(final Map<K, V> delegateMap) {
        return (ChangeNotifyingMap<K, V>) Proxy.newProxyInstance(SimpleChangeNotifyingMap.class
                .getClassLoader(), new Class[]{ChangeNotifyingMap.class}, new InvocationHandler() {

            private volatile boolean hasChanged = false;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if ("hasChanged".equals(method.getName())) {
                    return hasChanged;
                } else {
                    if (METHODS_OF_CHANGE.contains(method.getName())) {
                        hasChanged = true;
                    }
                    return method.invoke(delegateMap, args);
                }
            }
        });
    }
}
