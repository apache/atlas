/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.omrs.eventmanagement.events.v1;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEventType;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OMRSEventV1InstanceSection describes the properties specific to instance events
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OMRSEventV1InstanceSection implements Serializable
{
    private static final long serialVersionUID = 1L;

    private OMRSInstanceEventType eventType = null;

    private String         typeDefGUID                      = null;
    private String         typeDefName                      = null;
    private String         instanceGUID                     = null;
    private EntityDetail   entity                           = null;
    private Relationship   relationship                     = null;
    private String         homeMetadataCollectionId         = null;
    private String         originalHomeMetadataCollectionId = null;
    private TypeDefSummary originalTypeDefSummary           = null;
    private String         originalInstanceGUID             = null;

    public OMRSEventV1InstanceSection()
    {
    }

    public OMRSInstanceEventType getEventType()
    {
        return eventType;
    }

    public void setEventType(OMRSInstanceEventType eventType)
    {
        this.eventType = eventType;
    }

    public String getTypeDefGUID()
    {
        return typeDefGUID;
    }

    public void setTypeDefGUID(String typeDefGUID)
    {
        this.typeDefGUID = typeDefGUID;
    }

    public String getTypeDefName()
    {
        return typeDefName;
    }

    public void setTypeDefName(String typeDefName)
    {
        this.typeDefName = typeDefName;
    }

    public String getInstanceGUID()
    {
        return instanceGUID;
    }

    public void setInstanceGUID(String instanceGUID)
    {
        this.instanceGUID = instanceGUID;
    }

    public EntityDetail getEntity()
    {
        return entity;
    }

    public void setEntity(EntityDetail entity)
    {
        this.entity = entity;
    }

    public Relationship getRelationship()
    {
        return relationship;
    }

    public void setRelationship(Relationship relationship)
    {
        this.relationship = relationship;
    }

    public String getHomeMetadataCollectionId()
    {
        return homeMetadataCollectionId;
    }

    public void setHomeMetadataCollectionId(String homeMetadataCollectionId)
    {
        this.homeMetadataCollectionId = homeMetadataCollectionId;
    }

    public String getOriginalHomeMetadataCollectionId()
    {
        return originalHomeMetadataCollectionId;
    }

    public void setOriginalHomeMetadataCollectionId(String originalHomeMetadataCollectionId)
    {
        this.originalHomeMetadataCollectionId = originalHomeMetadataCollectionId;
    }

    public TypeDefSummary getOriginalTypeDefSummary()
    {
        return originalTypeDefSummary;
    }

    public void setOriginalTypeDefSummary(TypeDefSummary originalTypeDefSummary)
    {
        this.originalTypeDefSummary = originalTypeDefSummary;
    }

    public String getOriginalInstanceGUID()
    {
        return originalInstanceGUID;
    }

    public void setOriginalInstanceGUID(String originalInstanceGUID)
    {
        this.originalInstanceGUID = originalInstanceGUID;
    }
}
