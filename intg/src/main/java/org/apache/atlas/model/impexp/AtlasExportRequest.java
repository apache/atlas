/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.impexp;


import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;


@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasExportRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String OPTION_FETCH_TYPE      = "fetchType";
    public static final String OPTION_ATTR_MATCH_TYPE = "matchType";
    public static final String FETCH_TYPE_FULL        = "full";
    public static final String FETCH_TYPE_CONNECTED   = "connected";
    public static final String MATCH_TYPE_STARTS_WITH = "startsWith";
    public static final String MATCH_TYPE_ENDS_WITH   = "endsWith";
    public static final String MATCH_TYPE_CONTAINS    = "contains";
    public static final String MATCH_TYPE_MATCHES     = "matches";

    private List<AtlasObjectId> itemsToExport = new ArrayList<>();
    private Map<String, Object> options       = new HashMap<>();

    public List<AtlasObjectId> getItemsToExport() {
        return itemsToExport;
    }

    public void setItemsToExport(List<AtlasObjectId> itemsToExport) {
        this.itemsToExport = itemsToExport;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasExportRequest{");
        sb.append("itemsToExport={");
        AtlasBaseTypeDef.dumpObjects(itemsToExport, sb);
        sb.append("}");
        sb.append("options={");
        AtlasBaseTypeDef.dumpObjects(options, sb);
        sb.append("}");
        sb.append("}");

        return sb;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }
}
