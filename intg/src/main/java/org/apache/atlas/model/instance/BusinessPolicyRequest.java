/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Request to link/unlink policies from an asset.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class BusinessPolicyRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<AssetComplianceInfo> data = new ArrayList<>();

    public List<AssetComplianceInfo> getData() {
        return data;
    }

    public void setData(List<AssetComplianceInfo> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "LinkBusinessPolicyRequest{" +
                "data=" + data +
                '}';
    }

    public static class AssetComplianceInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private String assetId;

        private Set<String> addCompliantGUIDs = new HashSet<>();

        private Set<String> removeCompliantGUIDs = new HashSet<>();


        private Set<String> addNonCompliantGUIDs = new HashSet<>();

        private Set<String> removeNonCompliantGUIDs = new HashSet<>();


        public String getAssetId() {
            return assetId;
        }

        public void setAssetId(String assetId) {
            this.assetId = assetId;
        }

        public Set<String> getAddCompliantGUIDs() {
            return addCompliantGUIDs;
        }

        public void setAddCompliantGUIDs(Set<String> addCompliantGUIDs) {
            this.addCompliantGUIDs = addCompliantGUIDs;
        }

        public Set<String> getRemoveCompliantGUIDs() {
            return removeCompliantGUIDs;
        }

        public void setRemoveCompliantGUIDs(Set<String> removeCompliantGUIDs) {
            this.removeCompliantGUIDs = removeCompliantGUIDs;
        }

        public Set<String> getAddNonCompliantGUIDs() {
            return addNonCompliantGUIDs;
        }

        public void setAddNonCompliantGUIDs(Set<String> addNonCompliantGUIDs) {
            this.addNonCompliantGUIDs = addNonCompliantGUIDs;
        }

        public Set<String> getRemoveNonCompliantGUIDs() {
            return removeNonCompliantGUIDs;
        }

        public void setRemoveNonCompliantGUIDs(Set<String> removeNonCompliantGUIDs) {
            this.removeNonCompliantGUIDs = removeNonCompliantGUIDs;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("AssetComplianceInfo{");
            sb.append("assetId='").append(assetId).append('\'');
            sb.append(", addCompliantGUIDs=").append(addCompliantGUIDs);
            sb.append(", removeCompliantGUIDs=").append(removeCompliantGUIDs);
            sb.append(", addNonCompliantGUIDs=").append(addNonCompliantGUIDs);
            sb.append(", removeNonCompliantGUIDs=").append(removeNonCompliantGUIDs);
            sb.append('}');
            return sb.toString();
        }
    }
}
