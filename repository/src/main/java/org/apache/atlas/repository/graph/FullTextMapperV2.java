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
package org.apache.atlas.repository.graph;

import com.google.inject.Singleton;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Singleton
public class FullTextMapperV2 {
    private static final Logger LOG = LoggerFactory.getLogger(FullTextMapperV2.class);

    private static final String FULL_TEXT_DELIMITER         = " ";
    private static final String FULL_TEXT_FOLLOW_REFERENCES = "atlas.search.fulltext.followReferences";

    private final EntityGraphRetriever entityGraphRetriever;
    private final boolean              followReferences;

    @Inject
    public FullTextMapperV2(AtlasTypeRegistry typeRegistry) {
        this.entityGraphRetriever = new EntityGraphRetriever(typeRegistry);

        Configuration configuration = null;

        try {
            configuration = ApplicationProperties.get();
        } catch (Throwable e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("AtlasApplication properties couldn't be loaded", e);
            }
        } finally {
            followReferences = configuration != null && configuration.getBoolean(FULL_TEXT_FOLLOW_REFERENCES, false);
        }
    }

    public String map(String guid) throws AtlasBaseException {
        String                 ret     = null;
        RequestContext         context = RequestContext.get();
        AtlasEntityWithExtInfo entity  = context.getInstanceV2(guid);

        if (entity == null) {
            entity = entityGraphRetriever.toAtlasEntityWithExtInfo(guid);

            if (entity != null) {
                context.cache(entity);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cache miss -> GUID = {}", guid);
                }
            }
        }

        if (entity != null) {
            StringBuilder sb = new StringBuilder();

            map(entity.getEntity(), entity, sb, new HashSet<String>());

            ret = sb.toString();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("FullTextMapperV2.map({}): {}", guid, ret);
        }

        return ret;
    }

    private void map(AtlasEntity entity, AtlasEntityExtInfo entityExtInfo, StringBuilder sb, Set<String> processedGuids) throws AtlasBaseException {
        if (entity == null || processedGuids.contains(entity.getGuid())) {
            return;
        }

        processedGuids.add(entity.getGuid());

        sb.append(entity.getTypeName()).append(FULL_TEXT_DELIMITER);

        mapAttributes(entity.getAttributes(), entityExtInfo, sb, processedGuids);

        List<AtlasClassification> classifications = entity.getClassifications();
        if (CollectionUtils.isNotEmpty(classifications)) {
            for (AtlasClassification classification : classifications) {
                sb.append(classification.getTypeName()).append(FULL_TEXT_DELIMITER);

                mapAttributes(classification.getAttributes(), entityExtInfo, sb, processedGuids);
            }
        }
    }

    private void mapAttributes(Map<String, Object> attributes, AtlasEntityExtInfo entityExtInfo, StringBuilder sb, Set<String> processedGuids) throws AtlasBaseException {
        for (Map.Entry<String, Object> attributeEntry : attributes.entrySet()) {
            String attribKey = attributeEntry.getKey();
            Object attrValue = attributeEntry.getValue();

            if (attrValue == null) {
                continue;
            }

            sb.append(attribKey).append(FULL_TEXT_DELIMITER);

            mapAttribute(attrValue, entityExtInfo, sb, processedGuids);
        }
    }

    private void mapAttribute(Object value, AtlasEntityExtInfo entityExtInfo, StringBuilder sb, Set<String> processedGuids) throws AtlasBaseException {
        if (value instanceof AtlasObjectId) {
            if (followReferences) {
                AtlasObjectId objectId = (AtlasObjectId) value;
                AtlasEntity   entity   = entityExtInfo.getEntity(objectId.getGuid());

                if (entity != null) {
                    map(entity, entityExtInfo, sb, processedGuids);
                }
            }
        } else if (value instanceof List) {
            List valueList = (List) value;

            for (Object listElement : valueList) {
                mapAttribute(listElement, entityExtInfo, sb, processedGuids);
            }
        } else if (value instanceof Map) {
            Map valueMap = (Map) value;

            for (Object key : valueMap.keySet()) {
                mapAttribute(key, entityExtInfo, sb, processedGuids);
                mapAttribute(valueMap.get(key), entityExtInfo, sb, processedGuids);
            }
        } else if (value instanceof Enum) {
            Enum enumValue = (Enum) value;

            sb.append(enumValue.name()).append(FULL_TEXT_DELIMITER);
        } else if (value instanceof AtlasStruct) {
            AtlasStruct atlasStruct = (AtlasStruct) value;

            for (Map.Entry<String, Object> entry : atlasStruct.getAttributes().entrySet()) {
                sb.append(entry.getKey()).append(FULL_TEXT_DELIMITER);
                mapAttribute(entry.getValue(), entityExtInfo, sb, processedGuids);
            }
        } else {
            sb.append(String.valueOf(value)).append(FULL_TEXT_DELIMITER);
        }
    }
}
