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

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

public class AtlasEntityAccessRequest extends AtlasAccessRequest {
    private final AtlasEntityHeader   entity;
    private final String              entityId;
    private final AtlasClassification classification;
    private final String              attributeName;
    private final AtlasTypeRegistry   typeRegistry;
    private final Set<String>         entityClassifications;


    public AtlasEntityAccessRequest(AtlasTypeRegistry typeRegistry, AtlasPrivilege action) {
        this(typeRegistry, action, null, null, null, null, null);
    }

    public AtlasEntityAccessRequest(AtlasTypeRegistry typeRegistry, AtlasPrivilege action, AtlasEntityHeader entity) {
        this(typeRegistry, action, entity, null, null, null, null);
    }

    public AtlasEntityAccessRequest(AtlasTypeRegistry typeRegistry, AtlasPrivilege action, AtlasEntityHeader entity, AtlasClassification classification) {
        this(typeRegistry, action, entity, classification, null, null, null);
    }

    public AtlasEntityAccessRequest(AtlasTypeRegistry typeRegistry, AtlasPrivilege action, AtlasEntityHeader entity, String attributeName) {
        this(typeRegistry, action, entity, null, attributeName, null, null);
    }

    public AtlasEntityAccessRequest(AtlasTypeRegistry typeRegistry, AtlasPrivilege action, AtlasEntityHeader entity, String userName, Set<String> userGroups) {
        this(typeRegistry, action, entity, null, null, userName, userGroups);
    }

    public AtlasEntityAccessRequest(AtlasTypeRegistry typeRegistry, AtlasPrivilege action, AtlasEntityHeader entity, AtlasClassification classification, String userName, Set<String> userGroups) {
        this(typeRegistry, action, entity, classification, null, userName, userGroups);
    }

    public AtlasEntityAccessRequest(AtlasTypeRegistry typeRegistry, AtlasPrivilege action, AtlasEntityHeader entity, String attributeName, String userName, Set<String> userGroups) {
        this(typeRegistry, action, entity, null, attributeName, userName, userGroups);
    }

    public AtlasEntityAccessRequest(AtlasTypeRegistry typeRegistry, AtlasPrivilege action, AtlasEntityHeader entity, AtlasClassification classification, String attributeName, String userName, Set<String> userGroups) {
        super(action, userName, userGroups);

        this.entity                = entity;
        this.entityId              = super.getEntityId(entity, typeRegistry);
        this.classification        = classification;
        this.attributeName         = attributeName;
        this.typeRegistry          = typeRegistry;
        this.entityClassifications = super.getClassificationNames(entity);
    }

    public AtlasEntityHeader getEntity() {
        return entity;
    }

    public String getEntityId() {
        return entityId;
    }

    public AtlasClassification getClassification() {
        return classification;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getEntityType() {
        return entity == null ? StringUtils.EMPTY : entity.getTypeName();
    }

    public Set<String> getEntityClassifications() {
        return entityClassifications;
    }

    public Set<String> getEntityTypeAndAllSuperTypes() {
        return super.getEntityTypeAndAllSuperTypes(entity == null ? null : entity.getTypeName(), typeRegistry);
    }

    public Set<String> getClassificationTypeAndAllSuperTypes(String classificationName) {
        return super.getClassificationTypeAndAllSuperTypes(classificationName, typeRegistry);
    }

    @Override
    public String toString() {
        return "AtlasEntityAccessRequest[entity=" + entity + ", classification=" + classification + ", attributeName=" + attributeName +
                                         ", action=" + getAction() + ", accessTime=" + getAccessTime() + ", user=" + getUser() +
                                         ", userGroups=" + getUserGroups() + ", clientIPAddress=" + getClientIPAddress() + "]";
    }
}


