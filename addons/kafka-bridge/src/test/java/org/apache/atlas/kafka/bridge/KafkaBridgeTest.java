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

package org.apache.atlas.kafka.bridge;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.kafka.model.KafkaDataTypes;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.utils.KafkaUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;


import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class KafkaBridgeTest {

    private static final String TEST_TOPIC_NAME = "test_topic";
    public static final AtlasEntity.AtlasEntityWithExtInfo TOPIC_WITH_EXT_INFO = new AtlasEntity.AtlasEntityWithExtInfo(
            getTopicEntityWithGuid("0dd466a4-3838-4537-8969-6abb8b9e9185"));
    private static final String CLUSTER_NAME = "primary";
    private static final String TOPIC_QUALIFIED_NAME = KafkaBridge.getTopicQualifiedName(CLUSTER_NAME, TEST_TOPIC_NAME);

    @BeforeMethod
    public void initializeMocks() {
        MockitoAnnotations.initMocks(this);
    }

    private static AtlasEntity getTopicEntityWithGuid(String guid) {
        AtlasEntity ret = new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName());
        ret.setGuid(guid);
        return ret;
    }

    @Test
    public void testImportTopic() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockCreateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(TOPIC_WITH_EXT_INFO.getEntity().getGuid());
        when(mockCreateResponse.getCreatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.createEntity(any()))
                .thenReturn(mockCreateResponse);
        when(mockAtlasClientV2.getEntityByGuid(TOPIC_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(TOPIC_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        bridge.importTopic(TEST_TOPIC_NAME);

        ArgumentCaptor<AtlasEntity.AtlasEntityWithExtInfo> argumentCaptor = ArgumentCaptor.forClass(AtlasEntity.AtlasEntityWithExtInfo.class);
        verify(mockAtlasClientV2).createEntity(argumentCaptor.capture());
        AtlasEntity.AtlasEntityWithExtInfo entity = argumentCaptor.getValue();
        assertEquals(entity.getEntity().getAttribute("qualifiedName"), TOPIC_QUALIFIED_NAME);
    }

    @Test
    public void testCreateTopic() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockCreateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(TOPIC_WITH_EXT_INFO.getEntity().getGuid());
        when(mockCreateResponse.getCreatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.createEntity(any()))
                .thenReturn(mockCreateResponse);
        when(mockAtlasClientV2.getEntityByGuid(TOPIC_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(TOPIC_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createOrUpdateTopic(TEST_TOPIC_NAME);

        assertEquals(TOPIC_WITH_EXT_INFO, ret);
    }

    @Test
    public void testUpdateTopic() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockUpdateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(TOPIC_WITH_EXT_INFO.getEntity().getGuid());
        when(mockUpdateResponse.getUpdatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.getEntityByAttribute(eq(KafkaDataTypes.KAFKA_TOPIC.getName()), any()))
                .thenReturn(TOPIC_WITH_EXT_INFO);
        when(mockAtlasClientV2.updateEntity(any()))
                .thenReturn(mockUpdateResponse);
        when(mockAtlasClientV2.getEntityByGuid(TOPIC_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(TOPIC_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createOrUpdateTopic(TEST_TOPIC_NAME);

        assertEquals(TOPIC_WITH_EXT_INFO, ret);
    }
}