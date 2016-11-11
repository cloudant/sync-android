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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class EventBusTest {

    public EventBus bus;
    public TestBusSubscriber sub;

    private static class TestEvent {

    }

    private static class ExtendedTestEvent extends TestEvent {

    }

    private static class TestBusSubscriber {

        List<Object> eventsReceived = new ArrayList<Object>();

        protected void onEvent(Object event) {
            eventsReceived.add(event);
        }

        @Subscribe
        public void onEvent(TestEvent event) {
            onEvent((Object) event);
        }
    }

    @Before
    public void newBus() throws Exception {
        bus = new EventBus();
        sub = new TestBusSubscriber();
    }

    /**
     * Test that a subscriber registered on an event bus can receive a notification.
     */
    @Test
    public void register() throws Exception {
        bus.register(sub);
        assertTrue("There should be no events", sub.eventsReceived.isEmpty());

        // Post an event
        TestEvent e = new TestEvent();
        bus.post(e);

        assertEquals("There should be one event received", 1, sub.eventsReceived.size());
        assertEquals("The event received should be the one sent", e, sub.eventsReceived.get(0));
    }

    /**
     * Test that a subscriber does not get a notification for an event type that is not the one
     * subscribed to.
     *
     * @throws Exception
     */
    @Test
    public void notificationsAreFiltered() throws Exception {
        bus.register(sub);

        // Post an event (of type Object) - we are subscribed to TestEvent
        Object e = new Object();
        bus.post(e);

        assertTrue("There should be no events", sub.eventsReceived.isEmpty());
    }

    /**
     * Test that a subscriber does not get a notification for an event type that is not the one
     * subscribed to.
     *
     * @throws Exception
     */
    @Test
    public void notificationSubtype() throws Exception {
        bus.register(sub);

        // Post an event that is a subtype of the subscribed type
        ExtendedTestEvent e = new ExtendedTestEvent();
        bus.post(e);

        assertEquals("There should be one event received", 1, sub.eventsReceived.size());
        assertEquals("The event received should be the one sent", e, sub.eventsReceived.get(0));
    }

    /**
     * Test that after unregistering notifications are no longer received.
     *
     * @throws Exception
     */
    @Test
    public void unregister() throws Exception {
        // Do the register test
        register();

        // Now unregister
        bus.unregister(sub);

        // Post a second event
        TestEvent e = new TestEvent();
        bus.post(e);

        assertEquals("There should be only one event received", 1, sub.eventsReceived.size());
        assertNotEquals("The event received should not be the latest one sent", e, sub
                .eventsReceived.get(0));
    }

    /**
     * Test that multiple events are correctly posted and received.
     *
     * @throws Exception
     */
    @Test
    public void multipleEvents() throws Exception {
        bus.register(sub);

        // Generate n events
        int n = 10;
        List<TestEvent> events = new ArrayList<TestEvent>(n);
        for (int i = 0; i < n; i++) {
            events.add(new TestEvent());
        }

        // Post the events
        for (TestEvent e : events) {
            bus.post(e);
        }

        assertEquals("All the events should be received", events, sub.eventsReceived);
    }

    /**
     * Test that a subscriber throwing an exception is handled by the EventBus such that the
     * exception does not propagate and cause a failure.
     *
     * @throws Exception
     */
    @Test
    public void subscriberException() throws Exception {
        bus.register(new Object() {

            @Subscribe
            public void throwOnEvent(TestEvent event) throws Exception {
                throw new Exception("Test subscriber exception");
            }
        });

        // Post an event
        TestEvent e = new TestEvent();
        bus.post(e);

        // Test passes if no exception is thrown
    }

}
