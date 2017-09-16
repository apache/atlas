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
package org.apache.atlas.model.profile;

import org.apache.atlas.model.AtlasBaseModelObject;
import org.apache.atlas.model.discovery.SearchParameters;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.Serializable;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasUserSavedSearch extends AtlasBaseModelObject implements Serializable {
    private static final long serialVersionUID = 1L;

    private String           ownerName;
    private String           name;
    private SearchParameters searchParameters;


    public AtlasUserSavedSearch() {
        this(null, null, null);
    }

    public AtlasUserSavedSearch(String name, SearchParameters searchParameters) {
        this(null, name, searchParameters);
    }

    public AtlasUserSavedSearch(String ownerName, String name) {
        this(ownerName, name, null);
    }

    public AtlasUserSavedSearch(String ownerName, String name, SearchParameters searchParameters) {
        setOwnerName(ownerName);
        setName(name);
        setSearchParameters(searchParameters);
    }


    public String getOwnerName() {
        return this.ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SearchParameters getSearchParameters() {
        return searchParameters;
    }

    public void setSearchParameters(SearchParameters searchParameters) {
        this.searchParameters = searchParameters;
    }


    @Override
    public StringBuilder toString(StringBuilder sb) {
        sb.append(", ownerName=").append(ownerName);
        sb.append(", name=").append(name);
        sb.append(", searchParameters=");
        if (searchParameters == null) {
            sb.append("null");
        } else {
            searchParameters.toString(sb);
        }

        return sb;
    }
}
