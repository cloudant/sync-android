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

import com.cloudant.sync.event.notifications.Notification;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

public class EventBusTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public EventBus bus;
    public TestBusSubscriber sub;

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
        Notification e = new Notification() {
        };
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
        bus.register(new ThrowingSubscriber());

        // Post an event
        TestEvent e = new TestEvent();
        bus.post(e);

        // Test passes if no exception is thrown
    }

    /**
     * Test that a class with the access level private is rejected.
     * @throws Exception
     */
    @Test
    public  void privateSubscriber() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Event Subscriber class com.cloudant.sync.event.EventBusTest$PrivateSubscriber needs to be public");

        bus.register(new PrivateSubscriber());
    }

    /**
     * Test that a class with the access level protected is rejected.
     * @throws Exception
     */
    @Test
    public void protectedSubcriber() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Event Subscriber class com.cloudant.sync.event.EventBusTest$ProtectedSubScriber needs to be public");
        bus.register(new ProtectedSubScriber());
    }

    /**
     * Test that an anonymous inner class is rejected, this is because Anonymous inner classess cannot be public.
     * @throws Exception
     */
    @Test
    public void anonymousClass() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Event Subscriber class com.cloudant.sync.event.EventBusTest$2 needs to be public");
        bus.register(new Object(){
            @Subscribe
            public void doNothing(Object object){

            }
        });
    }

    /**
     * Test that a class with the default or "package protected" access scope is rejected.
     * @throws Exception
     */
    @Test
    public void packageProtectedSubScriber() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Event Subscriber class com.cloudant.sync.event.EventBusTest$PackageProtectedSubscriber needs to be public");

        bus.register(new PackageProtectedSubscriber());
    }

    /**
     * Test that a public class with a subscribing method with default or "package protected" access scope is rejected
     */
    @Test
    public void packageProtectedSubMethod(){
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Subscriber method com.cloudant.sync.event.EventBusTest$PackageProtectedMethodSubscriber#doNothing is required to be public");

        bus.register(new PackageProtectedMethodSubscriber());
    }

    /**
     * Test that a public class with a protected subscribing method is rejected.
     */
    @Test
    public void protectedSubMehtod(){
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Subscriber method com.cloudant.sync.event.EventBusTest$ProtectedMethodSub#doNothing is required to be public");
        bus.register(new ProtectedMethodSub());
    }

    /**
     * Test that a public class with a private subscribing method is rejected.
     */
    @Test
    public void privateSubMethod(){
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Subscriber method com.cloudant.sync.event.EventBusTest$PrivateMethodSub#doNothing is required to be public");
        bus.register(new PrivateMethodSub());
    }

    /**
     * Test that a public class with a public subscriber method with 2 parameters is rejected.
     */
    @Test
    public void subMethodWith2Parameters(){
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Subscriber method  com.cloudant.sync.event.EventBusTest$SubscriberMethod2Params#doNothing2Arg is required to have only 1 parameter");

        bus.register(new SubscriberMethod2Params());
    }

    /**
     * Test that a public class with a public subscriber method with 0 parameters is rejected.
     */
    @Test
    public void subMethodWith0Parameters(){
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Subscriber method  com.cloudant.sync.event.EventBusTest$SubscriberMethod0Params#doNothing0Arg is required to have only 1 parameter");

        bus.register(new SubscriberMethod0Params());
    }

    @Test
    public void subMethodWithObjectTypeParameter(){
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Subscriber method com.cloudant.sync.event.EventBusTest$UnassignableToNotificationSubscriber#doNothing parameter is required to be assignable com.cloudant.sync.event.notifications.Notification");

        bus.register(new UnassignableToNotificationSubscriber());
    }

    private static class TestEvent implements Notification {

    }

    private static class ExtendedTestEvent extends TestEvent {

    }

    public static class TestBusSubscriber {

        List<Object> eventsReceived = new ArrayList<Object>();

        protected void onEvent(Object event) {
            eventsReceived.add(event);
        }

        @Subscribe
        public void onEvent(TestEvent event) {
            onEvent((Object) event);
        }
    }

    public static class ThrowingSubscriber {

        @Subscribe
        public void throwOnEvent(TestEvent event) throws Exception {
            throw new Exception("Test subscriber exception");
        }
    }


    public static class SubscriberMethod2Params{
        @Subscribe
        public void doNothing2Arg(Notification notification, Notification notification2){

        }

    }

    public static class SubscriberMethod0Params{
        @Subscribe
        public void doNothing0Arg(){

        }

    }

    private static class PrivateSubscriber {

    }

    protected static class ProtectedSubScriber{
    }

    static class PackageProtectedSubscriber {
    }

    public static class PackageProtectedMethodSubscriber {
        @Subscribe
        void doNothing(Notification notification){

        }
    }

    public static class ProtectedMethodSub {
        @Subscribe
        protected void doNothing(Notification notification){

        }
    }

    public static class PrivateMethodSub {
        @Subscribe
        private void doNothing(Notification notification){

        }
    }

    public static class UnassignableToNotificationSubscriber{
        @Subscribe
        public void doNothing(Object object){

        }
    }

}
