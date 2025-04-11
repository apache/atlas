/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.model;

import org.apache.atlas.plugin.util.RangerUserStoreUtil;
import org.apache.htrace.shaded.fasterxml.jackson.annotation.JsonInclude;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class GroupInfo extends RangerBaseModelObject implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private String                  name;
    private String                  description;
    private Map<String, String>     otherAttributes;

    public GroupInfo() {
        this(null, null, null);
    }

    public GroupInfo(String name, String description, Map<String, String> otherAttributes) {
        setName(name);
        setDescription(description);
        setOtherAttributes(otherAttributes);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, String> getOtherAttributes() {
        return otherAttributes;
    }

    public void setOtherAttributes(Map<String, String> otherAttributes) {
        this.otherAttributes = otherAttributes == null ? new HashMap<>() : otherAttributes;
    }

    @Override
    public String toString() {
        return "{name=" + name
                + ", description=" + description
                + ", otherAttributes=" + RangerUserStoreUtil.getPrintableOptions(otherAttributes)
                + "}";
    }

}