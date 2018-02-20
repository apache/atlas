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
package org.apache.atlas.v1.model.notification;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static org.apache.atlas.model.notification.EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2;

/**
 * Entity v2 notification
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class EntityNotificationV2 extends EntityNotification implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum OperationType {
        ENTITY_CREATE, ENTITY_UPDATE, ENTITY_DELETE,
        CLASSIFICATION_ADD, CLASSIFICATION_DELETE, CLASSIFICATION_UPDATE
    }

    private AtlasEntity               entity;
    private OperationType             operationType;
    private List<AtlasClassification> classifications;

    public EntityNotificationV2() { }

    public EntityNotificationV2(AtlasEntity entity, OperationType operationType, List<AtlasClassification> classifications) {
        setEntity(entity);
        setOperationType(operationType);
        setClassifications(classifications);
        setType(ENTITY_NOTIFICATION_V2);
    }

    public AtlasEntity getEntity() {
        return entity;
    }

    public void setEntity(AtlasEntity entity) {
        this.entity = entity;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    public List<AtlasClassification> getClassifications() {
        return classifications;
    }

    public void setClassifications(List<AtlasClassification> classifications) {
        this.classifications = classifications;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        EntityNotificationV2 that = (EntityNotificationV2) o;
        return Objects.equals(entity, that.entity) &&
               operationType == that.operationType &&
               Objects.equals(classifications, that.classifications);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entity, operationType, classifications);
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("EntityNotificationV1{");
        super.toString(sb);
        sb.append(", entity=");
        if (entity != null) {
            entity.toString(sb);
        } else {
            sb.append(entity);
        }
        sb.append(", operationType=").append(operationType);
        sb.append(", classifications=[");
        AtlasBaseTypeDef.dumpObjects(classifications, sb);
        sb.append("]");
        sb.append("}");

        return sb;
    }
}