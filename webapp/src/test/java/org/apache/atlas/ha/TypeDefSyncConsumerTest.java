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
package org.apache.atlas.ha;

import org.apache.atlas.AtlasConstants;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.commons.configuration2.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TypeDefSyncConsumerTest {
    @Mock
    private KafkaNotification kafkaNotification;

    @Mock
    private AtlasTypeDefStore typeDefStore;

    @Mock
    private Configuration configuration;

    private AutoCloseable closeable;
    private String        previousPort;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);

        previousPort = System.getProperty(AtlasConstants.SYSTEM_PROPERTY_APP_PORT);
        System.setProperty(AtlasConstants.SYSTEM_PROPERTY_APP_PORT, "21000");

        when(configuration.getString(TypeDefSyncConsumer.TOPIC_CONFIG, TypeDefSyncConsumer.DEFAULT_TOPIC))
                .thenReturn("ATLAS_TYPEDEF_TEST_TOPIC");
        when(configuration.getStringArray(HAConfiguration.ATLAS_SERVER_IDS))
                .thenReturn(new String[] {"server1"});
        when(configuration.getString(HAConfiguration.ATLAS_SERVER_ADDRESS_PREFIX + "server1"))
                .thenReturn("127.0.0.1:21000");
    }

    @AfterMethod
    public void teardown() throws Exception {
        if (previousPort == null) {
            System.clearProperty(AtlasConstants.SYSTEM_PROPERTY_APP_PORT);
        } else {
            System.setProperty(AtlasConstants.SYSTEM_PROPERTY_APP_PORT, previousPort);
        }

        closeable.close();
    }

    @Test
    public void parseSignal_validPayload_parsesNodeAndTimestamp() throws Exception {
        Object parsed = parseSignal("serverA:12345");

        assertNotNull(parsed);
        assertEquals(getFieldValue(parsed, "nodeId"), "serverA");
        assertEquals(getFieldValue(parsed, "timestamp"), 12345L);
    }

    @Test
    public void parseSignal_invalidPayload_returnsNull() throws Exception {
        assertNull(parseSignal(null));
        assertNull(parseSignal("missing-separator"));
        assertNull(parseSignal("serverA:not-a-number"));
    }

    @Test
    public void start_isNoopAndDoesNotCreateConsumerThread() throws Exception {
        TypeDefSyncConsumer consumer = new TypeDefSyncConsumer(kafkaNotification, typeDefStore, configuration);

        consumer.start();

        Field consumerThreadField = TypeDefSyncConsumer.class.getDeclaredField("consumerThread");
        consumerThreadField.setAccessible(true);
        assertNull(consumerThreadField.get(consumer));
    }

    @Test
    public void getHandlerOrder_returnsDefaultMetadataOrder() {
        TypeDefSyncConsumer consumer = new TypeDefSyncConsumer(kafkaNotification, typeDefStore, configuration);

        assertEquals(consumer.getHandlerOrder(), TypeDefSyncConsumer.HandlerOrder.DEFAULT_METADATA_SERVICE.getOrder());
    }

    private static Object parseSignal(String payload) throws Exception {
        Method parseMethod = TypeDefSyncConsumer.class.getDeclaredMethod("parseSignal", String.class);
        parseMethod.setAccessible(true);

        return parseMethod.invoke(null, payload);
    }

    private static Object getFieldValue(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);

        return field.get(target);
    }
}
