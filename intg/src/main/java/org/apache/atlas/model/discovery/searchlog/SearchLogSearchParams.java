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

package org.apache.atlas.model.discovery.searchlog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.type.AtlasType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class SearchLogSearchParams {

    private static Map<String, Object> defaultSort = new HashMap<>();
    static {
        Map<String, Object> order = new HashMap<>();
        order.put("order", "desc");

        defaultSort.put("created", order);
    }

    private Map dsl;

    public void setDsl(Map dsl) {
        this.dsl = dsl;
    }

    public Map getDsl() {
        return this.dsl;
    }

    public String getQueryString() {
        if (this.dsl != null) {
            if (!this.dsl.containsKey("sort")) {
                dsl.put("sort", Collections.singleton(defaultSort));
            }
            return AtlasType.toJson(dsl);
        }
        return "";
    }
}
