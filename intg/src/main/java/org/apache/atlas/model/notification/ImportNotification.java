/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.notification;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.typedef.AtlasTypesDef;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Class representing atlas import notification, extending HookNotification.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class ImportNotification extends HookNotification implements Serializable {
    private static final long serialVersionUID = 1L;

    public ImportNotification() {
    }

    public ImportNotification(HookNotificationType type, String user) {
        super(type, user);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("ImportNotification{");
        sb.append("type=").append(type);
        sb.append(", user=").append(user);
        sb.append("}");

        return sb;
    }

    /**
     * Notification for type definitions import
     */
    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasTypeDefImportNotification extends HookNotification implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty
        private String importId;

        @JsonProperty
        private AtlasTypesDef typeDefinitionMap;

        public AtlasTypeDefImportNotification() {
        }

        public AtlasTypeDefImportNotification(String importId, String user, AtlasTypesDef typeDefinitionMap) {
            super(HookNotificationType.IMPORT_TYPE_DEF, user);
            this.typeDefinitionMap = typeDefinitionMap;
            this.importId = importId;
        }

        public AtlasTypesDef getTypeDefinitionMap() {
            return typeDefinitionMap;
        }

        public String getImportId() {
            return importId;
        }

        @Override
        public String toString() {
            return "importId=" + importId + (typeDefinitionMap == null ? "null" : typeDefinitionMap.toString());
        }
    }

    /**
     * Notification for entities import
     */
    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.ALWAYS)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasEntityImportNotification extends HookNotification implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty
        private String importId;

        @JsonProperty
        private AtlasEntityWithExtInfo entities;

        @JsonProperty
        private int position;

        public AtlasEntityImportNotification() {
        }

        public AtlasEntityImportNotification(String importId, String user, AtlasEntityWithExtInfo entities, int position) {
            super(HookNotificationType.IMPORT_ENTITY, user);
            this.entities = entities;
            this.position = position;
            this.importId = importId;
        }

        public AtlasEntityWithExtInfo getEntities() {
            return entities;
        }

        public int getPosition() {
            return position;
        }

        public String getImportId() {
            return importId;
        }

        @Override
        public String toString() {
            return "importId=" + importId + " position=" + position + "; entities=" + entities.toString();
        }
    }
}
