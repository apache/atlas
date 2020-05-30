/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.kafka;

import org.apache.atlas.AtlasException;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class KafkaNotificationMockTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateConsumers() throws Exception {
        Properties properties = mock(Properties.class);
        when(properties.getProperty("entities.group.id")).thenReturn("atlas");
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(KafkaNotification.ATLAS_ENTITIES_TOPIC, 1);

        final AtlasKafkaConsumer consumer1 = mock(AtlasKafkaConsumer.class);
        final AtlasKafkaConsumer consumer2 = mock(AtlasKafkaConsumer.class);

        KafkaNotification kafkaNotification =
                new TestKafkaNotification(properties, consumer1, consumer2);

        List<NotificationConsumer<AtlasKafkaConsumer>> consumers =
                kafkaNotification.createConsumers(NotificationInterface.NotificationType.ENTITIES, 2);

        assertEquals(consumers.size(), 2);
        assertTrue(consumers.contains(consumer1));
        assertTrue(consumers.contains(consumer2));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendMessagesSuccessfully() throws NotificationException,
            ExecutionException, InterruptedException {
        Properties configProperties = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer producer = mock(Producer.class);
        String topicName = kafkaNotification.getProducerTopicName(NotificationInterface.NotificationType.HOOK);
        String message = "This is a test message";
        Future returnValue = mock(Future.class);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        when(returnValue.get()).thenReturn(new RecordMetadata(topicPartition, 0, 0, 0, Long.valueOf(0), 0, 0));
        ProducerRecord expectedRecord = new ProducerRecord(topicName, message);
        when(producer.send(expectedRecord)).thenReturn(returnValue);

        kafkaNotification.sendInternalToProducer(producer,
                NotificationInterface.NotificationType.HOOK, Arrays.asList(new String[]{message}));

        verify(producer).send(expectedRecord);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowExceptionIfProducerFails() throws NotificationException,
            ExecutionException, InterruptedException {
        Properties configProperties = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer producer = mock(Producer.class);
        String topicName = kafkaNotification.getProducerTopicName(NotificationInterface.NotificationType.HOOK);
        String message = "This is a test message";
        Future returnValue = mock(Future.class);
        when(returnValue.get()).thenThrow(new RuntimeException("Simulating exception"));
        ProducerRecord expectedRecord = new ProducerRecord(topicName, message);
        when(producer.send(expectedRecord)).thenReturn(returnValue);

        try {
            kafkaNotification.sendInternalToProducer(producer,
                NotificationInterface.NotificationType.HOOK, Arrays.asList(new String[]{message}));
            fail("Should have thrown NotificationException");
        } catch (NotificationException e) {
            assertEquals(e.getFailedMessages().size(), 1);
            assertEquals(e.getFailedMessages().get(0), "This is a test message");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCollectAllFailedMessagesIfProducerFails() throws NotificationException,
            ExecutionException, InterruptedException {
        Properties configProperties = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer producer = mock(Producer.class);
        String topicName = kafkaNotification.getProducerTopicName(NotificationInterface.NotificationType.HOOK);
        String message1 = "This is a test message1";
        String message2 = "This is a test message2";
        Future returnValue1 = mock(Future.class);
        when(returnValue1.get()).thenThrow(new RuntimeException("Simulating exception"));
        Future returnValue2 = mock(Future.class);
        when(returnValue2.get()).thenThrow(new RuntimeException("Simulating exception"));
        ProducerRecord expectedRecord1 = new ProducerRecord(topicName, message1);
        when(producer.send(expectedRecord1)).thenReturn(returnValue1);
        ProducerRecord expectedRecord2 = new ProducerRecord(topicName, message2);
        when(producer.send(expectedRecord2)).thenReturn(returnValue1);

        try {
            kafkaNotification.sendInternalToProducer(producer,
                    NotificationInterface.NotificationType.HOOK, Arrays.asList(new String[]{message1, message2}));
            fail("Should have thrown NotificationException");
        } catch (NotificationException e) {
            assertEquals(e.getFailedMessages().size(), 2);
            assertEquals(e.getFailedMessages().get(0), "This is a test message1");
            assertEquals(e.getFailedMessages().get(1), "This is a test message2");
        }
    }

    @Test
    public void testSetKafkaJAASPropertiesForAllProperValues() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        try {
            KafkaNotification kafkaNotification = new KafkaNotification(configuration);
            kafkaNotification.setKafkaJAASProperties(configuration, properties);
            String newPropertyValue = properties.getProperty(KafkaNotification.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");
        } catch (AtlasException e) {
            fail("Failed while creating KafkaNotification object with exception : " + e.getMessage());
        }

    }

    @Test
    public void testSetKafkaJAASPropertiesForMissingControlFlag() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        try {
            KafkaNotification kafkaNotification = new KafkaNotification(configuration);
            kafkaNotification.setKafkaJAASProperties(configuration, properties);
            String newPropertyValue = properties.getProperty(KafkaNotification.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");
        } catch (AtlasException e) {
            fail("Failed while creating KafkaNotification object with exception : " + e.getMessage());
        }

    }

    @Test
    public void testSetKafkaJAASPropertiesForMissingLoginModuleName() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        try {
            KafkaNotification kafkaNotification = new KafkaNotification(configuration);
            kafkaNotification.setKafkaJAASProperties(configuration, properties);
            String newPropertyValue = properties.getProperty(KafkaNotification.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

            assertNull(newPropertyValue);
        } catch (AtlasException e) {
            fail("Failed while creating KafkaNotification object with exception : " + e.getMessage());
        }

    }

    @Test
    public void testSetKafkaJAASPropertiesWithSpecialCharacters() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionKeyTabPath = "/path/to/file.keytab";
        final String optionPrincipal = "test/_HOST@EXAMPLE.COM";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.keyTabPath", optionKeyTabPath);
        configuration.setProperty("atlas.jaas.KafkaClient.option.principal", optionPrincipal);

        try {
            KafkaNotification kafkaNotification = new KafkaNotification(configuration);
            kafkaNotification.setKafkaJAASProperties(configuration, properties);
            String newPropertyValue = properties.getProperty(KafkaNotification.KAFKA_SASL_JAAS_CONFIG_PROPERTY);
            String updatedPrincipalValue = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(optionPrincipal, (String) null);

            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("keyTabPath=\"" + optionKeyTabPath + "\""));
            assertTrue(newPropertyValue.contains("principal=\""+ updatedPrincipalValue + "\""));

        } catch (AtlasException e) {
            fail("Failed while creating KafkaNotification object with exception : " + e.getMessage());
        } catch (IOException e) {
            fail("Failed while getting updated principal value with exception : " + e.getMessage());
        }

    }

    @Test
    public void testSetKafkaJAASPropertiesForTicketBasedLoginConfig() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.serviceName",optionServiceName);

        try {
            KafkaNotification kafkaNotification = new KafkaNotification(configuration);
            KafkaNotification spyKafkaNotification = Mockito.spy(kafkaNotification);
            when(spyKafkaNotification.isLoginKeytabBased()).thenReturn(false);
            when(spyKafkaNotification.isLoginTicketBased()).thenReturn(true);
            spyKafkaNotification.setKafkaJAASProperties(configuration, properties);
            String newPropertyValue = properties.getProperty(KafkaNotification.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");
        } catch (AtlasException e) {
            fail("Failed while creating KafkaNotification object with exception : " + e.getMessage());
        }
    }

    @Test
    public void testSetKafkaJAASPropertiesForTicketBasedLoginFallback() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        try {
            KafkaNotification kafkaNotification = new KafkaNotification(configuration);
            KafkaNotification spyKafkaNotification = Mockito.spy(kafkaNotification);
            when(spyKafkaNotification.isLoginKeytabBased()).thenReturn(false);
            when(spyKafkaNotification.isLoginTicketBased()).thenReturn(true);
            spyKafkaNotification.setKafkaJAASProperties(configuration, properties);
            String newPropertyValue = properties.getProperty(KafkaNotification.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");
        } catch (AtlasException e) {
            fail("Failed while creating KafkaNotification object with exception : " + e.getMessage());
        }
    }

    class TestKafkaNotification extends KafkaNotification {

        private final AtlasKafkaConsumer consumer1;
        private final AtlasKafkaConsumer consumer2;

        TestKafkaNotification(Properties properties,
                              AtlasKafkaConsumer consumer1, AtlasKafkaConsumer consumer2) {
            super(properties);
            this.consumer1 = consumer1;
            this.consumer2 = consumer2;
        }


        @Override
        public <T> List<NotificationConsumer<T>> createConsumers(NotificationType notificationType,
                                                                 int numConsumers) {
            List consumerList = new ArrayList<NotificationConsumer>();
            consumerList.add(consumer1);
            consumerList.add(consumer2);
            return consumerList;
        }

        protected <T> AtlasKafkaConsumer<T>
        createConsumers(Class<T> type, int consumerId,  boolean autoCommitEnabled) {
            if (consumerId == 0) {
                return consumer1;
            } else if (consumerId == 1) {
                return consumer2;
            }
            return null;
        }


    }
}
