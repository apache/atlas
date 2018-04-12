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
package org.apache.atlas.model.metrics;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.HashMap;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Atlas metrics
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AtlasMetrics {
    private Map<String, Map<String, Object>> data;

    public AtlasMetrics() {
        setData(null);
    }

    public AtlasMetrics(Map<String, Map<String, Object>> data) {
        setData(data);
    }

    public AtlasMetrics(AtlasMetrics other) {
        if (other != null) {
            setData(other.getData());
        }
    }

    public Map<String, Map<String, Object>> getData() {
        return data;
    }

    public void setData(Map<String, Map<String, Object>> data) {
        this.data = data;
    }

    @JsonIgnore
    public void addMetric(String groupKey, String key, Object value) {
        Map<String, Map<String, Object>> data = this.data;
        if (data == null) {
            data = new HashMap<>();
        }
        Map<String, Object> metricMap = data.computeIfAbsent(groupKey, k -> new HashMap<>());
        metricMap.put(key, value);
        setData(data);
    }

    @JsonIgnore
    public Number getNumericMetric(String groupKey, String key) {
        Object metric = getMetric(groupKey, key);
        return metric instanceof Number ? (Number) metric : null;
    }

    @JsonIgnore
    public Object getMetric(String groupKey, String key) {
        Map<String, Map<String, Object>> data = this.data;
        Object ret = null;
        if (data != null) {
            Map<String, Object> metricMap = data.get(groupKey);
            if (metricMap != null && !metricMap.isEmpty()) {
                ret = metricMap.get(key);
            }
        }
        return ret;
    }
}
