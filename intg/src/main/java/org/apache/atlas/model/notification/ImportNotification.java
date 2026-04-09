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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
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
@JsonInclude
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class ImportNotification extends HookNotification implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty
    private String importId;

    protected ImportNotification() {
    }

    protected ImportNotification(HookNotificationType type, String user, String importId) {
        super(type, user);

        this.importId = importId;
    }

    public String getImportId() {
        return importId;
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("ImportNotification{");
        super.toString(sb);
        sb.append(", type=").append(type);
        sb.append(", user=").append(user);
        sb.append(", importId=").append(importId);
        sb.append("}");

        return sb;
    }

    /**
     * Notification for type definitions import
     */
    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasTypesDefImportNotification extends ImportNotification implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty
        private AtlasTypesDef typesDef;

        public AtlasTypesDefImportNotification() {
        }

        public AtlasTypesDefImportNotification(String importId, String user, AtlasTypesDef typesDef) {
            super(HookNotificationType.IMPORT_TYPES_DEF, user, importId);

            this.typesDef = typesDef;
        }

        public AtlasTypesDef getTypesDef() {
            return typesDef;
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("AtlasTypeDefImportNotification{");
            super.toString(sb);
            sb.append(", typesDef=").append(typesDef);
            sb.append("}");

            return sb;
        }
    }

    /**
     * Notification for entities import
     */
    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonInclude
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasEntityImportNotification extends ImportNotification implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty
        private AtlasEntityWithExtInfo entity;

        @JsonProperty
        private int position;

        public AtlasEntityImportNotification() {
        }

        public AtlasEntityImportNotification(String importId, String user, AtlasEntityWithExtInfo entity, int position) {
            super(HookNotificationType.IMPORT_ENTITY, user, importId);

            this.entity   = entity;
            this.position = position;
        }

        public AtlasEntityWithExtInfo getEntity() {
            return entity;
        }

        public int getPosition() {
            return position;
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("AtlasEntityImportNotification{");
            super.toString(sb);
            sb.append(", entity=").append(entity);
            sb.append(", position=").append(position);
            sb.append("}");

            return sb;
        }
    }
}
