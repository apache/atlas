/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.apache.atlas.notification.preprocessor.EntityPreprocessor.getQualifiedName;
import static org.testng.Assert.assertEquals;

public class GenericEntityPreprocessorTest {
    private final HookMessageDeserializer deserializer = new HookMessageDeserializer();

    public void testEntityTypesToIgnore(String msgJson, List<Pattern> entityTypesToIgnore) {
        PreprocessorContext context  = getPreprocessorContext(msgJson);
        List<AtlasEntity>   entities = context.getEntities();

        Set<String> filteredEntitiesActual = filterEntity(entities, (AtlasEntity entity) -> isMatch(entityTypesToIgnore, entity.getTypeName()));

        if (context.getReferredEntities() != null) {
            filteredEntitiesActual.addAll(filterEntity(new ArrayList<>(context.getReferredEntities().values()), (AtlasEntity entity) -> isMatch(entityTypesToIgnore, entity.getTypeName())));
        }

        GenericEntityPreprocessor entityPreprocessor = new GenericEntityPreprocessor(entityTypesToIgnore, Collections.emptyList());
        preprocessEntities(entityPreprocessor, context);

        assertEquals(filteredEntitiesActual, context.getIgnoredEntities());
    }

    public void testEntitiesToIgnoreByQName(String msgJson, List<Pattern> entitiesToIgnore) {
        PreprocessorContext context  = getPreprocessorContext(msgJson);
        List<AtlasEntity>   entities = context.getEntities();

        Set<String> filteredEntitiesActual = filterEntity(entities, (AtlasEntity entity) -> isMatch(entitiesToIgnore, getQualifiedName(entity)));
        if (context.getReferredEntities() != null) {
            filteredEntitiesActual.addAll(filterEntity(new ArrayList<>(context.getReferredEntities().values()), (AtlasEntity entity) -> isMatch(entitiesToIgnore, getQualifiedName(entity))));
        }

        GenericEntityPreprocessor entityPreprocessor = new GenericEntityPreprocessor(Collections.emptyList(), entitiesToIgnore);
        preprocessEntities(entityPreprocessor, context);

        assertEquals(filteredEntitiesActual, context.getIgnoredEntities());
    }

    public void testEntitiesToIgnoreByAndTypeQName(String msgJson, List<Pattern> entityTypesToIgnore, List<Pattern> entitiesToIgnore) {
        PreprocessorContext context  = getPreprocessorContext(msgJson);
        List<AtlasEntity>   entities = context.getEntities();

        Set<String> filteredEntitiesActual = filterEntity(entities, (AtlasEntity entity) ->
                isMatch(entityTypesToIgnore, entity.getTypeName()) && isMatch(entitiesToIgnore, getQualifiedName(entity)));

        if (context.getReferredEntities() != null) {
            filteredEntitiesActual.addAll(filterEntity(new ArrayList<>(context.getReferredEntities().values()), (AtlasEntity entity) ->
                    isMatch(entityTypesToIgnore, entity.getTypeName()) && isMatch(entitiesToIgnore, getQualifiedName(entity))));
        }

        GenericEntityPreprocessor entityPreprocessor = new GenericEntityPreprocessor(entityTypesToIgnore, entitiesToIgnore);
        preprocessEntities(entityPreprocessor, context);

        assertEquals(filteredEntitiesActual, context.getIgnoredEntities());
    }

    private PreprocessorContext getPreprocessorContext(String msgJson) {
        HookNotification hookNotification = deserializer.deserialize(msgJson);

        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(hookNotification, -1, KafkaNotification.ATLAS_HOOK_TOPIC, -1);

        return new PreprocessorContext(kafkaMsg, null, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(), false, false, true,
                false, null);
    }

    private boolean isMatch(List<Pattern> patterns, String property) {
        for (Pattern p : patterns) {
            if (p.matcher(property).matches()) {
                return true;
            }
        }
        return false;
    }

    private void preprocessEntities(GenericEntityPreprocessor genericEntityPreprocessor, PreprocessorContext context) {
        List<AtlasEntity> entities = context.getEntities();

        if (entities != null) {
            for (int i = 0; i < entities.size(); i++) {
                AtlasEntity entity = entities.get(i);
                genericEntityPreprocessor.preprocess(entity, context);

                if (context.isIgnoredEntity(entity.getGuid())) {
                    entities.remove(i--);
                }
            }
        }

        Map<String, AtlasEntity> referredEntities = context.getReferredEntities();

        if (referredEntities != null) {
            for (Iterator<Map.Entry<String, AtlasEntity>> iterator = referredEntities.entrySet().iterator(); iterator.hasNext(); ) {
                AtlasEntity entity = iterator.next().getValue();
                genericEntityPreprocessor.preprocess(entity, context);

                if (context.isIgnoredEntity(entity.getGuid())) {
                    iterator.remove();
                }
            }
        }
    }

    private Set<String> filterEntity(List<AtlasEntity> entities, Predicate<AtlasEntity> predicate) {
        Set<String> filteredEntitiesActual = new HashSet<>();

        if (entities != null) {
            for (AtlasEntity entity : entities) {
                if (predicate.test(entity)) {
                    filteredEntitiesActual.add(entity.getGuid());
                }
            }
        }

        return filteredEntitiesActual;
    }
}
