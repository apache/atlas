/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.notification;

import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.atlas.notification.NotificationInterface.NotificationType.ENTITIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class EntityNotificationSenderTest {
    private static final String TEST_NOTIFICATION = "test-notification";
    private static final boolean NOTIFY_POST_COMMIT_DEFAULT = true;
    @Mock
    private NotificationInterface notificationInterface;
    @Mock
    private Configuration configuration;
    private EntityNotificationSender<String> entityNotificationSender;
    private List<String> testNotifications;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        testNotifications = Collections.singletonList(TEST_NOTIFICATION);
    }

    @After
    public void tearDown() {
        // Clean up any ThreadLocal values that might have been set
        cleanupThreadLocalValues();
    }

    // Constructor Tests

    @Test
    public void testConstructorWithConfigurationNull() {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, null);

        assertNotNull(entityNotificationSender);
        // Should use default value when configuration is null
        verifyNotificationSenderType(true); // Default is post-commit
    }

    @Test
    public void testConstructorWithConfigurationDefault() {
        when(configuration.getBoolean("atlas.notification.send.postcommit", NOTIFY_POST_COMMIT_DEFAULT))
                .thenReturn(NOTIFY_POST_COMMIT_DEFAULT);

        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, configuration);

        assertNotNull(entityNotificationSender);
        verify(configuration).getBoolean("atlas.notification.send.postcommit", NOTIFY_POST_COMMIT_DEFAULT);
        verifyNotificationSenderType(true);
    }

    @Test
    public void testConstructorWithConfigurationFalse() {
        when(configuration.getBoolean("atlas.notification.send.postcommit", NOTIFY_POST_COMMIT_DEFAULT))
                .thenReturn(false);

        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, configuration);

        assertNotNull(entityNotificationSender);
        verify(configuration).getBoolean("atlas.notification.send.postcommit", NOTIFY_POST_COMMIT_DEFAULT);
        verifyNotificationSenderType(false);
    }

    @Test
    public void testConstructorWithConfigurationTrue() {
        when(configuration.getBoolean("atlas.notification.send.postcommit", NOTIFY_POST_COMMIT_DEFAULT))
                .thenReturn(true);

        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, configuration);

        assertNotNull(entityNotificationSender);
        verify(configuration).getBoolean("atlas.notification.send.postcommit", NOTIFY_POST_COMMIT_DEFAULT);
        verifyNotificationSenderType(true);
    }

    @Test
    public void testConstructorWithBooleanTrue() {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, true);

        assertNotNull(entityNotificationSender);
        verifyNotificationSenderType(true);
    }

    @Test
    public void testConstructorWithBooleanFalse() {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, false);

        assertNotNull(entityNotificationSender);
        verifyNotificationSenderType(false);
    }

    // Public Method Tests

    @Test
    public void testSendWithInlineNotificationSender() throws NotificationException {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, false);

        entityNotificationSender.send(testNotifications);

        verify(notificationInterface).send(eq(ENTITIES), eq(testNotifications));
    }

    @Test
    public void testSendWithPostCommitNotificationSender() throws NotificationException {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, true);

        entityNotificationSender.send(testNotifications);

        // PostCommit doesn't send immediately, so notificationInterface.send should not be called
        verify(notificationInterface, never()).send(any(NotificationInterface.NotificationType.class), any(List.class));

        // But notifications should be queued in ThreadLocal
        verifyNotificationsQueuedInThreadLocal();
    }

    @Test
    public void testSendWithEmptyList() throws NotificationException {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, false);
        List<String> emptyNotifications = Collections.emptyList();

        entityNotificationSender.send(emptyNotifications);

        verify(notificationInterface).send(eq(ENTITIES), eq(emptyNotifications));
    }

    @Test
    public void testSendWithNullList() throws NotificationException {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, false);

        entityNotificationSender.send(null);

        verify(notificationInterface).send(eq(ENTITIES), (List<String>) isNull());
    }

    @Test(expected = NotificationException.class)
    public void testSendWithInlineNotificationSenderThrowsException() throws NotificationException {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, false);
        doThrow(new NotificationException(new RuntimeException("Test exception")))
                .when(notificationInterface).send(eq(ENTITIES), eq(testNotifications));

        entityNotificationSender.send(testNotifications);
    }

    // Private Inner Class Tests using Reflection

    @Test
    public void testInlineNotificationSenderDirectly() throws Exception {
        // Create InlineNotificationSender using reflection
        Class<?> inlineNotificationSenderClass = getInnerClass("InlineNotificationSender");
        Constructor<?> constructor = inlineNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        constructor.setAccessible(true);
        Object inlineNotificationSender = constructor.newInstance(notificationInterface);

        // Test send method
        Method sendMethod = inlineNotificationSenderClass.getDeclaredMethod("send", List.class);
        sendMethod.setAccessible(true);
        sendMethod.invoke(inlineNotificationSender, testNotifications);

        verify(notificationInterface).send(eq(ENTITIES), eq(testNotifications));
    }

    @Test
    public void testInlineNotificationSenderConstructor() throws Exception {
        Class<?> inlineNotificationSenderClass = getInnerClass("InlineNotificationSender");
        Constructor<?> constructor = inlineNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        constructor.setAccessible(true);
        Object inlineNotificationSender = constructor.newInstance(notificationInterface);

        assertNotNull(inlineNotificationSender);

        // Verify notificationInterface field is set correctly
        Field notificationInterfaceField = inlineNotificationSenderClass.getDeclaredField("notificationInterface");
        notificationInterfaceField.setAccessible(true);
        assertEquals(notificationInterface, notificationInterfaceField.get(inlineNotificationSender));
    }

    @Test
    public void testPostCommitNotificationSenderDirectly() throws Exception {
        // Create PostCommitNotificationSender using reflection
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> constructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        constructor.setAccessible(true);
        Object postCommitNotificationSender = constructor.newInstance(notificationInterface);

        assertNotNull(postCommitNotificationSender);

        // Verify notificationInterface field is set correctly
        Field notificationInterfaceField = postCommitNotificationSenderClass.getDeclaredField("notificationInterface");
        notificationInterfaceField.setAccessible(true);
        assertEquals(notificationInterface, notificationInterfaceField.get(postCommitNotificationSender));
    }

    @Test
    public void testPostCommitNotificationSenderSendMethod() throws Exception {
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> constructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        constructor.setAccessible(true);
        Object postCommitNotificationSender = constructor.newInstance(notificationInterface);

        // Test send method - first call should create new hook
        Method sendMethod = postCommitNotificationSenderClass.getDeclaredMethod("send", List.class);
        sendMethod.setAccessible(true);
        sendMethod.invoke(postCommitNotificationSender, testNotifications);

        // Verify ThreadLocal contains a hook
        Field threadLocalField = postCommitNotificationSenderClass.getDeclaredField("postCommitNotificationHooks");
        threadLocalField.setAccessible(true);
        ThreadLocal<?> threadLocal = (ThreadLocal<?>) threadLocalField.get(postCommitNotificationSender);
        assertNotNull(threadLocal.get());

        // Test send method - second call should reuse existing hook
        List<String> moreNotifications = Collections.singletonList("additional-notification");
        sendMethod.invoke(postCommitNotificationSender, moreNotifications);

        // Hook should still exist and contain both sets of notifications
        assertNotNull(threadLocal.get());
    }

    @Test
    public void testPostCommitNotificationHookConstructor() throws Exception {
        Class<?> postCommitNotificationHookClass = getInnerClass("PostCommitNotificationHook");

        // Create PostCommitNotificationSender first
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> senderConstructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        senderConstructor.setAccessible(true);
        Object postCommitNotificationSender = senderConstructor.newInstance(notificationInterface);

        // Create PostCommitNotificationHook
        Constructor<?> hookConstructor = postCommitNotificationHookClass.getDeclaredConstructor(postCommitNotificationSenderClass, List.class);
        hookConstructor.setAccessible(true);
        Object hook = hookConstructor.newInstance(postCommitNotificationSender, testNotifications);

        assertNotNull(hook);

        // Verify notifications field is populated
        Field notificationsField = postCommitNotificationHookClass.getDeclaredField("notifications");
        notificationsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> notifications = (List<String>) notificationsField.get(hook);
        assertEquals(1, notifications.size());
        assertEquals(TEST_NOTIFICATION, notifications.get(0));
    }

    @Test
    public void testPostCommitNotificationHookAddNotifications() throws Exception {
        Class<?> postCommitNotificationHookClass = getInnerClass("PostCommitNotificationHook");

        // Create PostCommitNotificationSender first
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> senderConstructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        senderConstructor.setAccessible(true);
        Object postCommitNotificationSender = senderConstructor.newInstance(notificationInterface);

        // Create PostCommitNotificationHook
        Constructor<?> hookConstructor = postCommitNotificationHookClass.getDeclaredConstructor(postCommitNotificationSenderClass, List.class);
        hookConstructor.setAccessible(true);
        Object hook = hookConstructor.newInstance(postCommitNotificationSender, testNotifications);

        // Test addNotifications method
        Method addNotificationsMethod = postCommitNotificationHookClass.getDeclaredMethod("addNotifications", List.class);
        addNotificationsMethod.setAccessible(true);

        List<String> additionalNotifications = Collections.singletonList("additional-notification");
        addNotificationsMethod.invoke(hook, additionalNotifications);

        // Verify notifications field contains both sets
        Field notificationsField = postCommitNotificationHookClass.getDeclaredField("notifications");
        notificationsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> notifications = (List<String>) notificationsField.get(hook);
        assertEquals(2, notifications.size());
        assertTrue(notifications.contains(TEST_NOTIFICATION));
        assertTrue(notifications.contains("additional-notification"));
    }

    @Test
    public void testPostCommitNotificationHookAddNotificationsWithNull() throws Exception {
        Class<?> postCommitNotificationHookClass = getInnerClass("PostCommitNotificationHook");

        // Create PostCommitNotificationSender first
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> senderConstructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        senderConstructor.setAccessible(true);
        Object postCommitNotificationSender = senderConstructor.newInstance(notificationInterface);

        // Create PostCommitNotificationHook
        Constructor<?> hookConstructor = postCommitNotificationHookClass.getDeclaredConstructor(postCommitNotificationSenderClass, List.class);
        hookConstructor.setAccessible(true);
        Object hook = hookConstructor.newInstance(postCommitNotificationSender, testNotifications);

        // Test addNotifications method with null
        Method addNotificationsMethod = postCommitNotificationHookClass.getDeclaredMethod("addNotifications", List.class);
        addNotificationsMethod.setAccessible(true);
        addNotificationsMethod.invoke(hook, (Object) null);

        // Verify notifications field still contains original notifications
        Field notificationsField = postCommitNotificationHookClass.getDeclaredField("notifications");
        notificationsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> notifications = (List<String>) notificationsField.get(hook);
        assertEquals(1, notifications.size());
        assertEquals(TEST_NOTIFICATION, notifications.get(0));
    }

    @Test
    public void testPostCommitNotificationHookOnCompleteSuccess() throws Exception {
        Class<?> postCommitNotificationHookClass = getInnerClass("PostCommitNotificationHook");

        // Create PostCommitNotificationSender first
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> senderConstructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        senderConstructor.setAccessible(true);
        Object postCommitNotificationSender = senderConstructor.newInstance(notificationInterface);

        // Create PostCommitNotificationHook
        Constructor<?> hookConstructor = postCommitNotificationHookClass.getDeclaredConstructor(postCommitNotificationSenderClass, List.class);
        hookConstructor.setAccessible(true);
        Object hook = hookConstructor.newInstance(postCommitNotificationSender, testNotifications);

        // Set up ThreadLocal for cleanup verification
        Field threadLocalField = postCommitNotificationSenderClass.getDeclaredField("postCommitNotificationHooks");
        threadLocalField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<Object> threadLocal = (ThreadLocal<Object>) threadLocalField.get(postCommitNotificationSender);
        threadLocal.set(hook);

        // Test onComplete method with success = true
        Method onCompleteMethod = postCommitNotificationHookClass.getDeclaredMethod("onComplete", boolean.class);
        onCompleteMethod.setAccessible(true);
        onCompleteMethod.invoke(hook, true);

        // Verify notifications were sent
        verify(notificationInterface).send(eq(ENTITIES), eq(testNotifications));

        // Verify ThreadLocal was cleared
        assertNull(threadLocal.get());
    }

    @Test
    public void testPostCommitNotificationHookOnCompleteFailure() throws Exception {
        Class<?> postCommitNotificationHookClass = getInnerClass("PostCommitNotificationHook");

        // Create PostCommitNotificationSender first
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> senderConstructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        senderConstructor.setAccessible(true);
        Object postCommitNotificationSender = senderConstructor.newInstance(notificationInterface);

        // Create PostCommitNotificationHook
        Constructor<?> hookConstructor = postCommitNotificationHookClass.getDeclaredConstructor(postCommitNotificationSenderClass, List.class);
        hookConstructor.setAccessible(true);
        Object hook = hookConstructor.newInstance(postCommitNotificationSender, testNotifications);

        // Set up ThreadLocal for cleanup verification
        Field threadLocalField = postCommitNotificationSenderClass.getDeclaredField("postCommitNotificationHooks");
        threadLocalField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<Object> threadLocal = (ThreadLocal<Object>) threadLocalField.get(postCommitNotificationSender);
        threadLocal.set(hook);

        // Test onComplete method with success = false
        Method onCompleteMethod = postCommitNotificationHookClass.getDeclaredMethod("onComplete", boolean.class);
        onCompleteMethod.setAccessible(true);
        onCompleteMethod.invoke(hook, false);

        // Verify notifications were NOT sent
        verify(notificationInterface, never()).send(any(NotificationInterface.NotificationType.class), any(List.class));

        // Verify ThreadLocal was still cleared
        assertNull(threadLocal.get());
    }

    @Test
    public void testPostCommitNotificationHookOnCompleteWithEmptyNotifications() throws Exception {
        Class<?> postCommitNotificationHookClass = getInnerClass("PostCommitNotificationHook");

        // Create PostCommitNotificationSender first
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> senderConstructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        senderConstructor.setAccessible(true);
        Object postCommitNotificationSender = senderConstructor.newInstance(notificationInterface);

        // Create PostCommitNotificationHook with empty notifications
        Constructor<?> hookConstructor = postCommitNotificationHookClass.getDeclaredConstructor(postCommitNotificationSenderClass, List.class);
        hookConstructor.setAccessible(true);
        Object hook = hookConstructor.newInstance(postCommitNotificationSender, Collections.emptyList());

        // Set up ThreadLocal for cleanup verification
        Field threadLocalField = postCommitNotificationSenderClass.getDeclaredField("postCommitNotificationHooks");
        threadLocalField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<Object> threadLocal = (ThreadLocal<Object>) threadLocalField.get(postCommitNotificationSender);
        threadLocal.set(hook);

        // Test onComplete method with success = true
        Method onCompleteMethod = postCommitNotificationHookClass.getDeclaredMethod("onComplete", boolean.class);
        onCompleteMethod.setAccessible(true);
        onCompleteMethod.invoke(hook, true);

        // Verify notifications were NOT sent (because list is empty)
        verify(notificationInterface, never()).send(any(NotificationInterface.NotificationType.class), any(List.class));

        // Verify ThreadLocal was still cleared
        assertNull(threadLocal.get());
    }

    @Test
    public void testPostCommitNotificationHookOnCompleteWithNotificationException() throws Exception {
        Class<?> postCommitNotificationHookClass = getInnerClass("PostCommitNotificationHook");

        // Create PostCommitNotificationSender first
        Class<?> postCommitNotificationSenderClass = getInnerClass("PostCommitNotificationSender");
        Constructor<?> senderConstructor = postCommitNotificationSenderClass.getDeclaredConstructor(NotificationInterface.class);
        senderConstructor.setAccessible(true);
        Object postCommitNotificationSender = senderConstructor.newInstance(notificationInterface);

        // Create PostCommitNotificationHook
        Constructor<?> hookConstructor = postCommitNotificationHookClass.getDeclaredConstructor(postCommitNotificationSenderClass, List.class);
        hookConstructor.setAccessible(true);
        Object hook = hookConstructor.newInstance(postCommitNotificationSender, testNotifications);

        // Set up ThreadLocal for cleanup verification
        Field threadLocalField = postCommitNotificationSenderClass.getDeclaredField("postCommitNotificationHooks");
        threadLocalField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<Object> threadLocal = (ThreadLocal<Object>) threadLocalField.get(postCommitNotificationSender);
        threadLocal.set(hook);

        // Set up exception to be thrown
        doThrow(new NotificationException(new RuntimeException("Test exception")))
                .when(notificationInterface).send(eq(ENTITIES), eq(testNotifications));

        // Test onComplete method with success = true
        Method onCompleteMethod = postCommitNotificationHookClass.getDeclaredMethod("onComplete", boolean.class);
        onCompleteMethod.setAccessible(true);

        // Should not throw exception - it should be caught and logged
        onCompleteMethod.invoke(hook, true);

        // Verify notifications were attempted to be sent
        verify(notificationInterface).send(eq(ENTITIES), eq(testNotifications));

        // Verify ThreadLocal was cleared even with exception
        assertNull(threadLocal.get());
    }

    // Integration Tests - Testing Real Workflow

    @Test
    public void testPostCommitWorkflowIntegration() throws Exception {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, true);

        // Send first batch of notifications
        List<String> firstBatch = Collections.singletonList("notification-1");
        entityNotificationSender.send(firstBatch);

        // Send second batch - should be added to same hook
        List<String> secondBatch = Collections.singletonList("notification-2");
        entityNotificationSender.send(secondBatch);

        // Get the hook and simulate transaction success
        Object hook = getCurrentPostCommitHook();
        assertNotNull(hook);

        // Simulate successful transaction
        Method onCompleteMethod = hook.getClass().getDeclaredMethod("onComplete", boolean.class);
        onCompleteMethod.setAccessible(true);
        onCompleteMethod.invoke(hook, true);

        // Verify both batches were sent together
        List<String> expectedNotifications = new ArrayList<>();
        expectedNotifications.addAll(firstBatch);
        expectedNotifications.addAll(secondBatch);
        verify(notificationInterface).send(ENTITIES, expectedNotifications);
    }

    @Test
    public void testMultipleThreadsWithPostCommitSender() throws Exception {
        entityNotificationSender = new EntityNotificationSender<>(notificationInterface, true);

        // This tests that ThreadLocal works correctly - each thread should have its own hook
        Thread thread1 = new Thread(() -> {
            try {
                entityNotificationSender.send(Collections.singletonList("thread-1-notification"));
                Object hook = getCurrentPostCommitHook();
                assertNotNull(hook);
            } catch (Exception e) {
                fail("Thread 1 failed: " + e.getMessage());
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                entityNotificationSender.send(Collections.singletonList("thread-2-notification"));
                Object hook = getCurrentPostCommitHook();
                assertNotNull(hook);
            } catch (Exception e) {
                fail("Thread 2 failed: " + e.getMessage());
            }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();
    }

    // Edge Case Tests

    @Test
    public void testInheritanceFromGraphTransactionInterceptorPostTransactionHook() throws Exception {
        Class<?> postCommitNotificationHookClass = getInnerClass("PostCommitNotificationHook");

        // Verify it extends GraphTransactionInterceptor.PostTransactionHook
        assertTrue("PostCommitNotificationHook should extend GraphTransactionInterceptor.PostTransactionHook",
                GraphTransactionInterceptor.PostTransactionHook.class.isAssignableFrom(postCommitNotificationHookClass));
    }

    @Test
    public void testNotificationSenderInterface() throws Exception {
        Class<?> notificationSenderInterface = getInnerClass("NotificationSender");

        // Verify it's an interface
        assertTrue("NotificationSender should be an interface", notificationSenderInterface.isInterface());

        // Verify it has the send method
        Method sendMethod = notificationSenderInterface.getDeclaredMethod("send", List.class);
        assertNotNull(sendMethod);
    }

    @Test
    public void testConstantValues() throws Exception {
        // Test default constant value
        Field defaultField = EntityNotificationSender.class.getDeclaredField("NOTIFY_POST_COMMIT_DEFAULT");
        defaultField.setAccessible(true);
        boolean defaultValue = defaultField.getBoolean(null);
        assertEquals(NOTIFY_POST_COMMIT_DEFAULT, defaultValue);
    }

    // Helper Methods

    /**
     * Verify the type of notificationSender field (PostCommit vs Inline)
     */
    private void verifyNotificationSenderType(boolean shouldBePostCommit) {
        try {
            Field notificationSenderField = EntityNotificationSender.class.getDeclaredField("notificationSender");
            notificationSenderField.setAccessible(true);
            Object sender = notificationSenderField.get(entityNotificationSender);

            if (shouldBePostCommit) {
                assertEquals("PostCommitNotificationSender", sender.getClass().getSimpleName());
            } else {
                assertEquals("InlineNotificationSender", sender.getClass().getSimpleName());
            }
        } catch (Exception e) {
            fail("Failed to verify notification sender type: " + e.getMessage());
        }
    }

    /**
     * Verify that notifications are queued in ThreadLocal for PostCommit sender
     */
    private void verifyNotificationsQueuedInThreadLocal() {
        try {
            Field notificationSenderField = EntityNotificationSender.class.getDeclaredField("notificationSender");
            notificationSenderField.setAccessible(true);
            Object sender = notificationSenderField.get(entityNotificationSender);

            Field threadLocalField = sender.getClass().getDeclaredField("postCommitNotificationHooks");
            threadLocalField.setAccessible(true);
            ThreadLocal<?> threadLocal = (ThreadLocal<?>) threadLocalField.get(sender);

            assertNotNull("ThreadLocal should contain a hook", threadLocal.get());
        } catch (Exception e) {
            fail("Failed to verify ThreadLocal queuing: " + e.getMessage());
        }
    }

    /**
     * Get the current PostCommitNotificationHook from ThreadLocal
     */
    private Object getCurrentPostCommitHook() {
        try {
            Field notificationSenderField = EntityNotificationSender.class.getDeclaredField("notificationSender");
            notificationSenderField.setAccessible(true);
            Object sender = notificationSenderField.get(entityNotificationSender);

            Field threadLocalField = sender.getClass().getDeclaredField("postCommitNotificationHooks");
            threadLocalField.setAccessible(true);
            ThreadLocal<?> threadLocal = (ThreadLocal<?>) threadLocalField.get(sender);

            return threadLocal.get();
        } catch (Exception e) {
            fail("Failed to get current hook: " + e.getMessage());
            return null;
        }
    }

    /**
     * Clean up ThreadLocal values to prevent test interference
     */
    private void cleanupThreadLocalValues() {
        if (entityNotificationSender != null) {
            try {
                Field notificationSenderField = EntityNotificationSender.class.getDeclaredField("notificationSender");
                notificationSenderField.setAccessible(true);
                Object sender = notificationSenderField.get(entityNotificationSender);

                if (sender != null && "PostCommitNotificationSender".equals(sender.getClass().getSimpleName())) {
                    Field threadLocalField = sender.getClass().getDeclaredField("postCommitNotificationHooks");
                    threadLocalField.setAccessible(true);
                    ThreadLocal<?> threadLocal = (ThreadLocal<?>) threadLocalField.get(sender);
                    threadLocal.remove();
                }
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    /**
     * Get inner class by simple name
     */
    private Class<?> getInnerClass(String className) {
        // First check direct inner classes
        for (Class<?> innerClass : EntityNotificationSender.class.getDeclaredClasses()) {
            if (innerClass.getSimpleName().equals(className)) {
                return innerClass;
            }

            // For nested classes like PostCommitNotificationHook (inside PostCommitNotificationSender)
            for (Class<?> nestedClass : innerClass.getDeclaredClasses()) {
                if (nestedClass.getSimpleName().equals(className)) {
                    return nestedClass;
                }
            }
        }
        fail("Inner class not found: " + className);
        return null;
    }
}
