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
package org.apache.atlas.authorize;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasAccessorRequest {

    private String action;

    private String guid;
    private String typeName;
    private String qualifiedName;
    private String label;
    private String classification;
    private String businessMetadata;

    private String relationshipTypeName;
    private String entityQualifiedNameEnd1;
    private String entityQualifiedNameEnd2;
    private String entityGuidEnd1;
    private String entityGuidEnd2;
    private String entityTypeEnd1;
    private String entityTypeEnd2;

    private AtlasEntityHeader entity = null;

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getClassification() {
        return classification;
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getBusinessMetadata() {
        return businessMetadata;
    }

    public void setBusinessMetadata(String businessMetadata) {
        this.businessMetadata = businessMetadata;
    }

    public AtlasEntityHeader getEntity() {
        return entity;
    }

    public void setEntity(AtlasEntityHeader entity) {
        this.entity = entity;
    }

    public String getRelationshipTypeName() {
        return relationshipTypeName;
    }

    public String getEntityQualifiedNameEnd1() {
        return entityQualifiedNameEnd1;
    }

    public String getEntityQualifiedNameEnd2() {
        return entityQualifiedNameEnd2;
    }

    public String getEntityGuidEnd1() {
        return entityGuidEnd1;
    }

    public String getEntityGuidEnd2() {
        return entityGuidEnd2;
    }

    public String getEntityTypeEnd1() {
        return entityTypeEnd1;
    }

    public String getEntityTypeEnd2() {
        return entityTypeEnd2;
    }

    @Override
    public String toString() {
        return "AtlasAccessor{" +
                "guid='" + guid + '\'' +
                ", typeName='" + typeName + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", label='" + label + '\'' +
                ", classification='" + classification + '\'' +
                ", businessMetadata='" + businessMetadata + '\'' +
                '}';
    }
}

