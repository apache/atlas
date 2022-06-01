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

package org.apache.atlas.model.tasks;

import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TaskSearchParams extends SearchParams {
    private static final Logger LOG = LoggerFactory.getLogger(TaskSearchParams.class);

    private Map dsl;

    public void setDsl(Map dsl) {
        this.dsl = dsl;
    }

    public Map getDsl() {
        return this.dsl;
    }

    @Override
    public String getQuery() {
        if (dsl != null) {
            if (!dsl.containsKey("sort")) {
                dsl.put("sort", Collections.singleton(mapOf("__task_timestamp", mapOf("order", "desc"))));
            }

            try {
                Map query = (Map) dsl.get("query");
                if (query.containsKey("match_all")) {
                    dsl.put("query", mapOf("exists", mapOf("field", "__task_v_type")));
                }
                LOG.info("dsl {}", dsl);
            } catch (Exception e) {
                LOG.warn("Failed to verify match_all clause in query");
            }

            return AtlasType.toJson(dsl);
        }
        return "";
    }

    private Map mapOf(String key, Object value) {
        Map ret = new HashMap();
        ret.put(key, value);

        return ret;
    }
}