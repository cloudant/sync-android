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

package com.cloudant.sync.event;

import com.cloudant.sync.event.notifications.Notification;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * A publish/subscribe event bus for sync notifications.
 * </p>
 * <p>
 * This class provides methods for registering and un-registering event subscribers as well as a
 * method for posting an event to the bus. Events are isolated to each specific instance of this
 * class.
 * </p>
 */
public class EventBus {

    private static final Logger LOGGER = Logger.getLogger(EventBus.class.getName());

    // A map of listeners to their subscribed methods.
    private final Map<Object, List<SubscriberMethod>> listeners = new ConcurrentHashMap
            <Object, List<SubscriberMethod>>();

    /**
     * Post an event to the bus. All subscribers to the event class type posted will be notified.
     *
     * @param event to post to subscribers
     */
    public void post(Notification event) {
        for (Map.Entry<Object, List<SubscriberMethod>> entry : listeners.entrySet()) {
            for (SubscriberMethod method : entry.getValue()) {
                if (method.eventTypeToInvokeOn.isInstance(event)) {
                    try {
                        method.methodToInvokeOnEvent.invoke(entry.getKey(), event);
                    } catch (InvocationTargetException e) {
                        // We log this exception and swallow it because we need to ensure we don't
                        // prevent completion of notifications if one listener is badly behaved and
                        // throws an exception of some kind.
                        LOGGER.log(Level.SEVERE, "Subscriber invocation failed for method \""
                                + method.toString() + "\"", e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(EventBus.class.getName() + " could not access " +
                                "subscriber " + method.toString(), e);
                    }
                }
            }
        }
    }

    /**
     * Register an instance containing one or more subscriber methods to receive events.
     * <p>
     * To receive events the instance must have 1 or more methods annotated with
     * {@link com.cloudant.sync.event.Subscribe}.
     * </p>
     *
     * <p>
     *     The following restrictions are placed on the subscriber instance.
     * </p>
     * <ul>
     *    <li>The class of the subscriber <strong>must</strong> be public</li>
     *    <li>Methods annotated with <code>@Subscribe</code> <strong>must</strong> be public</li>
     *    <li>Methods annotated with <code>@Subscribe</code> <strong>must</strong> have exactly <strong>one</strong> parameter</li>
     * </ul>
     *
     * @param object the instance that will be notified when an event is posted
     * @throws IllegalArgumentException if the subscribing class does not adhere to the restrictions listed.
     *
     */
    public void register(Object object) {

        Class<?> listenerClass = object.getClass();

        if(!Modifier.isPublic(listenerClass.getModifiers())){
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Event Subscriber class %s needs to be public",
                    listenerClass.getName()));
        }

        List<SubscriberMethod> methods = new ArrayList<SubscriberMethod>();
        // Do loop to traverse the class hierarchy looking for @Subscribe methods. Using
        // listenerClass.getMethods() seems preferable as it should include inherited public
        // methods, but it does not seem to work for some of the mock(listener) types used in
        // the tests (e.g. mock(StrategyListener.class)).
        do {
            for (Method m : listenerClass.getDeclaredMethods()) {
                if (m.isAnnotationPresent(com.cloudant.sync.event.Subscribe.class)) {

                    if (!Modifier.isPublic(m.getModifiers())){
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                "Subscriber method %s#%s is required to be public",
                                listenerClass.getName(), m.getName()));
                    }

                    Class[] params = m.getParameterTypes();
                    if (params.length == 1) {
                        if(!Notification.class.isAssignableFrom(params[0])){
                            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                    "Subscriber method %s#%s parameter is required to be assignable %s",
                                    listenerClass.getName(),
                                    m.getName(),
                                    Notification.class.getCanonicalName()));
                        }
                        methods.add(new SubscriberMethod(m, params[0]));
                    } else {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                "Subscriber method  %s#%s is required to have only 1 parameter",
                                listenerClass.getName(),
                                m.getName()));
                    }
                }
            }
        } while ((listenerClass = listenerClass.getSuperclass()) != null);

        listeners.put(object, methods);
    }

    /**
     * Deregister a previously registered instance with subscriber methods.
     *
     * @param object the instance that will no longer be notified of events
     */
    public void unregister(Object object) {
        listeners.remove(object);
    }

    /**
     * Class that encapsulates a {@link com.cloudant.sync.event.Subscribe} annotated method and the
     * type of event it is subscribed to (as discovered from its single parameter type).
     */
    private static final class SubscriberMethod {
        private final Method methodToInvokeOnEvent;
        private final Class<?> eventTypeToInvokeOn;

        private SubscriberMethod(Method methodToInvokeOnEvent, Class<?> eventTypeToInvokeOn) {
            this.methodToInvokeOnEvent = methodToInvokeOnEvent;
            this.eventTypeToInvokeOn = eventTypeToInvokeOn;
        }

        @Override
        public String toString() {
            return methodToInvokeOnEvent.toString();
        }
    }
}
