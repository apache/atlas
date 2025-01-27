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
package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Request to link/unlink policies from asset.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class LinkMeshEntityRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private Set<String> assetGuids;
    private String domainGuid; 


    public Set<String> getAssetGuids() {
        return assetGuids;
    }

    public void setAssetGuids(Set<String> assetGuids) {
        this.assetGuids = assetGuids;
    }

    public String getDomainGuid() {
        return domainGuid;
    }

    public void setDomainGuid(String domainGuid) {
        this.domainGuid = domainGuid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LinkBusinessPolicyRequest{");
        sb.append("assetGuids=").append(assetGuids);
        sb.append(", domainGuid='").append(domainGuid).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
