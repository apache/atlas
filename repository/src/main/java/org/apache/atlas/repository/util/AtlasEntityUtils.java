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

package org.apache.atlas.repository.util;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

final public class AtlasEntityUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityUtils.class);

    private AtlasEntityUtils() {
    }

    public static String getQualifiedName(AtlasEntity entity) {
        return getStringAttribute(entity, QUALIFIED_NAME);
    }

    public static String getName(AtlasEntity entity) {
        return getStringAttribute(entity, NAME);
    }

    public static List<String> getListAttribute(AtlasStruct entity, String attrName) {
        List<String> ret = new ArrayList<>();

        Object valueObj = entity.getAttribute(attrName);
        if (valueObj != null) {
            ret = (List<String>) valueObj;
        }

        return ret;
    }

    public static String getStringAttribute(AtlasEntity entity, String attrName) {
        Object obj = entity.getAttribute(attrName);
        return obj == null ? null : (String) obj;
    }

    public static String getStringAttribute(AtlasEntityHeader entity, String attrName) {
        Object obj = entity.getAttribute(attrName);
        return obj == null ? null : (String) obj;
    }

    public static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
}
