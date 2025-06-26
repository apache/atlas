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

package org.apache.atlas.notification.preprocessor;

import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.hook.HookMessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public class SparkPreprocessorTest {
    private static final Logger LOG = LoggerFactory.getLogger(SparkPreprocessorTest.class);
    private final HookMessageDeserializer deserializer = new HookMessageDeserializer();
    public static final String TYPE_SPARK_PROCESS = "spark_process";
    public static final String ATTRIBUTE_DETAILS = "details";
    public static final String ATTRIBUTE_SPARKPLANDESCRIPTION = "sparkPlanDescription";
    public static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
    public static final String ATTRIBUTE_NAME = "name";
    public static final String ATTRIBUTE_ISINCOMPLETE = "isIncomplete";
    public static final String ATTRIBUTE_REMOTEUSER = "remoteUser";
    public static final String ATTRIBUTE_EXECUTIONID = "executionId";
    public static final String ATTRIBUTE_QUERYTEXT = "queryText";
    public static final String ATTRIBUTE_CURRUSER = "currUser";
    public static final String ATTRIBUTE_GUID = "guid";
    private static final List<String> EMPTY_STR_LIST = new ArrayList<>();
    private static final List<Pattern> EMPTY_PATTERN_LIST = new ArrayList<>();

    private void getPreprocessorContext(AtlasEntity entity) {
        EntityPreprocessor preprocessor = EntityPreprocessor.getSparkPreprocessor(entity.getTypeName());
        HookNotification hookNotification = new HookNotification.EntityCreateRequestV2("test", new AtlasEntity.AtlasEntitiesWithExtInfo(entity));

        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage(hookNotification, -1, KafkaNotification.ATLAS_HOOK_TOPIC, -1);

        PreprocessorContext context = new PreprocessorContext(kafkaMsg, null, EMPTY_PATTERN_LIST, EMPTY_PATTERN_LIST, null,
                EMPTY_STR_LIST, EMPTY_STR_LIST, EMPTY_STR_LIST, false,
                false, true, false, null);
        if (preprocessor != null) {
            preprocessor.preprocess(entity, context);
        }
    }

    public Object[][] provideSparkProcessData() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTRIBUTE_NAME, "execution-1");
        attributes.put(ATTRIBUTE_QUALIFIED_NAME, "application_1740993925593_0006-execution-1sparkTab1");
        attributes.put(ATTRIBUTE_DETAILS, "== Parsed Logical Plan ==\\nCreateHiveTableAsSelectCommand ...");
        attributes.put(ATTRIBUTE_SPARKPLANDESCRIPTION, "Execute CreateHiveTableAsSelectCommand ...");
        attributes.put(ATTRIBUTE_GUID, "-32055574130361399");
        attributes.put(ATTRIBUTE_ISINCOMPLETE, "false");
        attributes.put(ATTRIBUTE_REMOTEUSER, "spark");
        attributes.put(ATTRIBUTE_EXECUTIONID, 1);
        attributes.put(ATTRIBUTE_QUERYTEXT, null);
        attributes.put(ATTRIBUTE_CURRUSER, "spark");

        return new Object[][] {{attributes}};
    }

    @Test
    public void replaceAttributesInSparkProcess() {
        Object[][] testData = provideSparkProcessData();

        for (Object[] data : testData) {
            Map<String, Object> attributes = (Map<String, Object>) data[0]; // Extract attributes

            AtlasEntity atlasEntity = new AtlasEntity();
            atlasEntity.setTypeName(TYPE_SPARK_PROCESS);
            attributes.forEach(atlasEntity::setAttribute);
            getPreprocessorContext(atlasEntity);

            assertEquals(atlasEntity.getAttribute(ATTRIBUTE_DETAILS), null);
            assertEquals(atlasEntity.getAttribute(ATTRIBUTE_SPARKPLANDESCRIPTION), null);
        }
    }
}
