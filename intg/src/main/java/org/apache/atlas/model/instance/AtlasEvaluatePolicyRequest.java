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
package org.apache.atlas.model.instance;

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * Request to run state-check of entities
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
    public class AtlasEvaluatePolicyRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String typeName;
    private String entityGuid;
    private String entityId;
    private String action;

    private String relationShipTypeName;

    private String entityTypeEnd1;
    private String entityGuidEnd1;
    private String entityIdEnd1;

    private String entityTypeEnd2;
    private String entityGuidEnd2;
    private String entityIdEnd2;


    private String classification;


    public AtlasEvaluatePolicyRequest() {
    }

    public String getEntityGuid() {
        return entityGuid;
    }

    public void setEntityGuid(String entityGuid) {
        this.entityGuid = entityGuid;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }


    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getEntityTypeEnd1() {
        return entityTypeEnd1;
    }

    public void setEntityTypeEnd1(String entityTypeEnd1) {
        this.entityTypeEnd1 = entityTypeEnd1;
    }

    public String getEntityGuidEnd1() {
        return entityGuidEnd1;
    }

    public void setEntityGuidEnd1(String entityGuidEnd1) {
        this.entityGuidEnd1 = entityGuidEnd1;
    }

    public String getEntityIdEnd1() {
        return entityIdEnd1;
    }

    public void setEntityIdEnd1(String entityIdEnd1) {
        this.entityIdEnd1 = entityIdEnd1;
    }

    public String getEntityTypeEnd2() {
        return entityTypeEnd2;
    }

    public void setEntityTypeEnd2(String entityTypeEnd2) {
        this.entityTypeEnd2 = entityTypeEnd2;
    }

    public String getEntityGuidEnd2() {
        return entityGuidEnd2;
    }

    public void setEntityGuidEnd2(String entityGuidEnd2) {
        this.entityGuidEnd2 = entityGuidEnd2;
    }

    public String getEntityIdEnd2() {
        return entityIdEnd2;
    }

    public void setEntityIdEnd2(String entityIdEnd2) {
        this.entityIdEnd2 = entityIdEnd2;
    }

    public String getRelationShipTypeName() {
        return relationShipTypeName;
    }

    public void setRelationShipTypeName(String relationShipTypeName) {
        this.relationShipTypeName = relationShipTypeName;
    }


    public String getClassification() {
        return classification;
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasEvaluatePolicyRequest{");
        sb.append("entityGuid=");
        sb.append(entityGuid);
        sb.append(", action=").append(action);
        sb.append(", typeName=").append(typeName);
        sb.append("}");

        return sb;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }
}
