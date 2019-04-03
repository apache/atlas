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
package org.apache.atlas.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Atlas statistics
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasStatistics {
    public static final String STAT_SERVER_START_TS                = "serverStartTimeStamp";
    public static final String STAT_SERVER_ACTIVE_TS               = "serverActiveTimeStamp";
    public static final String STAT_SERVER_UP_SINCE                = "serverUpTime";
    public static final String STAT_START_OFFSET                   = "KafkaTopic:ATLAS_HOOK:startOffset";
    public static final String STAT_CURRENT_OFFSET                 = "KafkaTopic:ATLAS_HOOK:currentOffset";
    public static final String STAT_SOLR_STATUS                    = "solrConnectionStatus";
    public static final String STAT_HBASE_STATUS                   = "HBaseConnectionStatus";
    public static final String STAT_LAST_MESSAGE_PROCESSED_TIME_TS = "lastMessageProcessedTimeStamp";
    public static final String STAT_AVG_MESSAGE_PROCESSING_TIME    = "avgMessageProcessingTime";

    private Map<String, Object> data = new HashMap<>();

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "AtlasStatistics{" +
                "data=" + data +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasStatistics other = (AtlasStatistics) o;

        return Objects.equals(this.data, other.data);
    }
}
