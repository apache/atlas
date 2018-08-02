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
package org.apache.atlas.model.clusterinfo;

import org.apache.atlas.model.AtlasBaseModelObject;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasCluster extends AtlasBaseModelObject implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String SYNC_INFO_KEY            = "syncInfo";
    public static final String OPERATION                = "operation";
    public static final String NEXT_MODIFIED_TIMESTAMP  = "nextModifiedTimestamp";

    private String name;
    private String qualifiedName;
    private Map<String, String> additionalInfo;
    private List<String> urls;

    public AtlasCluster() {
        urls = new ArrayList<>();
    }

    public AtlasCluster(String name, String qualifiedName) {
        this.name = name;
        this.qualifiedName = qualifiedName;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setAdditionalInfo(Map<String, String> additionalInfo) {
        if(this.additionalInfo == null) {
            this.additionalInfo = new HashMap<>();
        }

        this.additionalInfo = additionalInfo;
    }

    public void setAdditionalInfo(String key, String value) {
        if(this.additionalInfo == null) {
            this.additionalInfo = new HashMap<>();
        }

        additionalInfo.put(key, value);
    }

    public Map<String, String> getAdditionalInfo() {
        return this.additionalInfo;
    }

    public String getAdditionalInfo(String key) {
        return additionalInfo.get(key);
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public void setUrls(List<String> urls) {
        this.urls = urls;
    }

    public List<String> getUrls() {
        return this.urls;
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        sb.append(", name=").append(name);
        sb.append(", qualifiedName=").append(getQualifiedName());
        sb.append(", urls=").append(urls);
        sb.append(", additionalInfo=").append(additionalInfo);
        sb.append("}");
        return sb;
    }
}
