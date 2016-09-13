/*
 * Copyright (C) 2016 IBM Corp. All rights reserved.
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

import com.google.common.eventbus.Subscribe;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A publish/subscribe event bus for sync notifications.
 * <p>
 * This class provides methods for registering and un-registering event subscribers as well as a
 * method for posting an event to the bus. Events are isolated to each specific instance of this
 * class.
 * </p>
 * @api_public
 */
public class EventBus {

    // The Guava EventBus that this class delegates to.
    private final com.google.common.eventbus.EventBus eventBus = new com.google.common.eventbus
            .EventBus();
    // A map of listeners to their Guava bus proxies.
    private final Map<Object, BusProxy> listeners = new ConcurrentHashMap
            <Object, BusProxy>();

    /**
     * Post an event to the bus. All subscribers to the event class type posted will be notified.
     *
     * @param event to post to subscribers
     */
    public void post(Object event) {
        eventBus.post(event);
    }

    /**
     * Register an instance containing one or more subscriber methods to receive events.
     * <p>
     * To receive events the instance must have 1 or more methods annotated with
     * {@link com.cloudant.sync.event.Subscribe}.
     * </p>
     *
     * @param object the instance that will be notified when an event is posted
     */
    public void register(Object object) {
        BusProxy proxy = new BusProxy(object);
        BusProxy existingProxy = listeners.put(object, proxy);
        if (existingProxy != null) {
            // If the same object was registered twice unregister the previous proxy from the Guava
            // bus to prevent double notifications.
            eventBus.unregister(existingProxy);
        }
        eventBus.register(proxy);
    }

    /**
     * Deregister a previously registered instance with subscriber methods.
     *
     * @param object the instance that will no longer be notified of events
     */
    public void unregister(Object object) {
        BusProxy proxy = listeners.remove(object);
        if (proxy != null) {
            eventBus.unregister(proxy);
        }
    }

    /**
     * This class serves as a proxy for any listener registered with the sync EventBus. The proxy is
     * registered with the Guava EventBus (using an Object event type to receive all events).
     * The proxy then filters events as appropriate before passing them on to the methods that were
     * subscribed using the sync {@link com.cloudant.sync.event.Subscribe} annotation.
     * <p>
     * Instances of this class are registered on the Guava event bus for each listener registered on
     * the sync EventBus class.
     * </p>
     */
    private static final class BusProxy {

        // The instance registered wiht the sync EventBus
        private final Object listener;
        // The list of methods in that instance that were annotated with @Subscribe
        private final List<SubscriberMethod> methods = new ArrayList<SubscriberMethod>();

        // Constructor that reflectively identifies the @Subscribe annotated methods
        BusProxy(Object listener) {
            this.listener = listener;
            Class<?> listenerClass = listener.getClass();
            // Do loop to traverse the class hierarchy looking for @Subscribe methods. Using
            // listenerClass.getMethods() seems preferable as it should include inherited public
            // methods, but it does not seem to work for some of the mock(listener) types used in
            // the tests (e.g. mock(StrategyListener.class)).
            do {
                for (Method m : listenerClass.getDeclaredMethods()) {
                    if (m.isAnnotationPresent(com.cloudant.sync.event.Subscribe.class)) {
                        Class[] params = m.getParameterTypes();
                        if (params.length == 1) {
                            methods.add(new SubscriberMethod(m, params[0]));
                        }
                    }
                }
            } while ((listenerClass = listenerClass.getSuperclass()) != null);
        }

        /**
         * This method is annotated with Guava's @Subscribe annotation and uses an Object type so it
         * receives all notifications. It performs an instance check on received events and
         * invokes the appropriate proxy subscriber methods.
         *
         * @param o
         */
        @Subscribe
        public void onEvent(Object o) {
            for (SubscriberMethod method : methods) {
                if (method.eventTypeToInvokeOn.isInstance(o)) {
                    try {
                        method.methodToInvokeOnEvent.invoke(listener, o);
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException(EventBus.class.getName() + " could not invoke " +
                                "subscriber " + method.toString(), e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(EventBus.class.getName() + " could not access " +
                                "subscriber " + method.toString(), e);
                    }
                }
            }
        }
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
    }
}
