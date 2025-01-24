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
package org.apache.atlas.utils;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AtlasEntityUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityUtil.class);

    public static final String CONF_METADATA_NAMESPACE = "atlas.metadata.namespace";
    public static final String CLUSTER_NAME_KEY        = "atlas.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME    = "default";

    protected static final String   ENTITY          = "entity";
    protected static final String   ACTIVE          = "Active";
    protected static final String   DELETED         = "Deleted";
    protected static final String   SHELL           = "Shell";
    protected static final String[] STATUS_CATEGORY = {ACTIVE, DELETED, SHELL};

    private static final String SOFT_REFERENCE_FORMAT_SEPERATOR       = ":";
    private static final String SOFT_REF_FORMAT                       = "%s" + SOFT_REFERENCE_FORMAT_SEPERATOR + "%s";
    private static final int    SOFT_REFERENCE_FORMAT_INDEX_TYPE_NAME = 0;
    private static final int    SOFT_REFERENCE_FORMAT_INDEX_GUID      = 1;

    public static String formatSoftRefValue(String typeName, String guid) {
        return String.format(SOFT_REF_FORMAT, typeName, guid);
    }

    public static String formatSoftRefValue(AtlasObjectId objectId) {
        return formatSoftRefValue(objectId.getTypeName(), objectId.getGuid());
    }

    private AtlasEntityUtil() {
        // to block instantiation
    }

    public static List<String> formatSoftRefValue(List<AtlasObjectId> objIds) {
        List<String> ret = new ArrayList<>();

        for (AtlasObjectId objId : objIds) {
            ret.add(formatSoftRefValue(objId));
        }

        return ret;
    }

    public static Map<String, String> formatSoftRefValue(Map<String, AtlasObjectId> objIdMap) {
        Map<String, String> ret = new HashMap<>();

        for (Map.Entry<String, AtlasObjectId> entry : objIdMap.entrySet()) {
            ret.put(entry.getKey(), formatSoftRefValue(entry.getValue()));
        }

        return ret;
    }

    public static AtlasObjectId parseSoftRefValue(String softRefValue) {
        AtlasObjectId ret = null;

        if (StringUtils.isNotEmpty(softRefValue)) {
            String[] objectIdParts = StringUtils.split(softRefValue, SOFT_REFERENCE_FORMAT_SEPERATOR);

            if (objectIdParts.length >= 2) {
                ret = new AtlasObjectId(objectIdParts[SOFT_REFERENCE_FORMAT_INDEX_GUID], objectIdParts[SOFT_REFERENCE_FORMAT_INDEX_TYPE_NAME]);
            } else {
                LOG.warn("Invalid soft-ref value: {}", softRefValue);
            }
        }

        return ret;
    }

    public static List<AtlasObjectId> parseSoftRefValue(List<String> softRefValue) {
        List<AtlasObjectId> ret = null;

        if (CollectionUtils.isNotEmpty(softRefValue)) {
            ret = new ArrayList<>();

            for (String elemValue : softRefValue) {
                AtlasObjectId objId = parseSoftRefValue(elemValue);

                if (objId != null) {
                    ret.add(objId);
                }
            }
        }

        return ret;
    }

    public static Map<String, AtlasObjectId> parseSoftRefValue(Map<String, String> softRefValue) {
        Map<String, AtlasObjectId> ret = null;

        if (MapUtils.isNotEmpty(softRefValue)) {
            ret = new HashMap<>();

            for (Map.Entry<String, String> entry : softRefValue.entrySet()) {
                AtlasObjectId objId = parseSoftRefValue(entry.getValue());

                if (objId != null) {
                    ret.put(entry.getKey(), objId);
                }
            }
        }

        return ret;
    }

    public static String getRelationshipType(Object val) {
        final String ret;

        if (val instanceof AtlasRelatedObjectId) {
            ret = ((AtlasRelatedObjectId) val).getRelationshipType();
        } else if (val instanceof Collection) {
            String elemRelationshipType = null;

            for (Object elem : (Collection<?>) val) {
                elemRelationshipType = getRelationshipType(elem);

                if (elemRelationshipType != null) {
                    break;
                }
            }

            ret = elemRelationshipType;
        } else if (val instanceof Map) {
            Map<?, ?> mapValue = (Map<?, ?>) val;

            if (mapValue.containsKey(AtlasRelatedObjectId.KEY_RELATIONSHIP_TYPE)) {
                Object relTypeName = ((Map<?, ?>) val).get(AtlasRelatedObjectId.KEY_RELATIONSHIP_TYPE);

                ret = relTypeName != null ? relTypeName.toString() : null;
            } else {
                String entryRelationshipType = null;

                for (Object entryVal : mapValue.values()) {
                    entryRelationshipType = getRelationshipType(entryVal);

                    if (entryRelationshipType != null) {
                        break;
                    }
                }

                ret = entryRelationshipType;
            }
        } else {
            ret = null;
        }

        return ret;
    }

    public static String getMetadataNamespace() {
        String ret = StringUtils.EMPTY;
        try {
            ret = ApplicationProperties.get().getString(CONF_METADATA_NAMESPACE, StringUtils.EMPTY);
            if (StringUtils.isEmpty(ret)) {
                ret = ApplicationProperties.get().getString(CLUSTER_NAME_KEY, DEFAULT_CLUSTER_NAME);
            }
        } catch (AtlasException e) {
            LOG.info("Failed to load application properties", e);
        }
        return StringUtils.isNotEmpty(ret) ? ret : DEFAULT_CLUSTER_NAME;
    }

    public static void metricsToTypeData(AtlasMetrics metrics, String typeName, Map<String, Object> typeData) {
        if (typeData == null) {
            return;
        }

        Map<String, Integer> innerVal = new HashMap<>();

        for (String status : STATUS_CATEGORY) {
            Map<String, Integer> metricsMap = (Map<String, Integer>) metrics.getData().get(ENTITY).get(ENTITY + status);
            innerVal.put(status, metricsMap.getOrDefault(typeName, 0));
        }

        typeData.put(typeName, innerVal);
    }

    public static void metricsToTypeData(AtlasMetrics metrics, List<String> typeNames, Map<String, Object> typeData) {
        if (typeData == null || CollectionUtils.isEmpty(typeNames)) {
            return;
        }

        for (String typeName : typeNames) {
            metricsToTypeData(metrics, typeName, typeData);
        }
    }
}
