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
package org.apache.atlas.utils;


import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;


public class AtlasEntityUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityUtil.class);

    public static boolean hasAnyAttributeUpdate(AtlasEntityType entityType, AtlasEntity currEntity, AtlasEntity newEntity) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> hasAnyAttributeUpdate(guid={}, typeName={})", currEntity.getGuid(), currEntity.getTypeName());
        }

        boolean ret = false;

        for (AtlasAttribute attribute : entityType.getAllAttributes().values()) {
            String    attrName  = attribute.getName();
            AtlasType attrType  = attribute.getAttributeType();
            Object    currValue = attrType.getNormalizedValue(currEntity.getAttribute(attrName));
            Object    newValue  = attrType.getNormalizedValue(newEntity.getAttribute(attrName));

            if (!Objects.equals(currValue, newValue)) {
                ret = true;

                // for map/list types, treat 'null' same as empty
                if ((currValue == null && newValue != null) || (currValue != null && newValue == null)) {
                    if (attrType instanceof AtlasMapType) {
                        if (MapUtils.isEmpty((Map) currValue) && MapUtils.isEmpty((Map) newValue)) {
                            ret = false;
                        }
                    } else if (attrType instanceof AtlasArrayType) {
                        if (CollectionUtils.isEmpty((Collection) currValue) && CollectionUtils.isEmpty((Collection) newValue)) {
                            ret = false;
                        }
                    }
                }

                if (ret) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("hasAnyAttributeUpdate(guid={}, typeName={}): attribute '{}' is found updated - currentValue={}, newValue={}",
                                  currEntity.getGuid(), currEntity.getTypeName(), attrName, currValue, newValue);
                    }

                    break;
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== hasAnyAttributeUpdate(guid={}, typeName={}): ret={}", currEntity.getGuid(), currEntity.getTypeName(), ret);
        }

        return ret;
    }
}
