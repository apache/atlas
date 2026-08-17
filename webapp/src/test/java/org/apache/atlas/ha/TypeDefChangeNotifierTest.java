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
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.commons.configuration2.Configuration;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TypeDefChangeNotifierTest {
    @Mock
    private KafkaNotification kafkaNotification;

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
    public void onChange_nullPayload_doesNotPublishSignal() throws Exception {
        TypeDefChangeNotifier notifier = new TypeDefChangeNotifier(kafkaNotification, configuration);

        notifier.onChange(null);

        verify(kafkaNotification, never()).sendInternal(eq("ATLAS_TYPEDEF_TEST_TOPIC"), org.mockito.Matchers.anyList());
    }

    @Test
    public void onChange_emptyChanges_doesNotPublishSignal() throws Exception {
        TypeDefChangeNotifier notifier = new TypeDefChangeNotifier(kafkaNotification, configuration);

        notifier.onChange(new ChangedTypeDefs());

        verify(kafkaNotification, never()).sendInternal(eq("ATLAS_TYPEDEF_TEST_TOPIC"), org.mockito.Matchers.anyList());
    }

    @Test
    public void onChange_withChanges_publishesTimestampedSignal() throws Exception {
        TypeDefChangeNotifier notifier = new TypeDefChangeNotifier(kafkaNotification, configuration);
        ChangedTypeDefs      changes  = new ChangedTypeDefs();
        changes.setCreatedTypeDefs(Collections.singletonList(org.mockito.Mockito.mock(AtlasBaseTypeDef.class)));

        ArgumentCaptor<List> payloadCaptor = ArgumentCaptor.forClass(List.class);

        notifier.onChange(changes);

        verify(kafkaNotification).sendInternal(eq("ATLAS_TYPEDEF_TEST_TOPIC"), payloadCaptor.capture());
        assertEquals(payloadCaptor.getValue().size(), 1);

        String payload = String.valueOf(payloadCaptor.getValue().get(0));
        assertTrue(payload.startsWith("server1:"));
        assertTrue(payload.split(":").length >= 2);
    }
}
