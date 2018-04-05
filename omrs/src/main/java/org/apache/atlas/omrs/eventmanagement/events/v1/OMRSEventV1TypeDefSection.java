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
import org.apache.atlas.omrs.eventmanagement.events.OMRSTypeDefEventType;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefPatch;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OMRSEventV1TypeDefSection describes the properties specific to TypeDef related events
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OMRSEventV1TypeDefSection implements Serializable
{
    private static final long serialVersionUID = 1L;

    private OMRSTypeDefEventType typeDefEventType         = null;
    private String               typeDefGUID              = null;
    private String               typeDefName              = null;
    private AttributeTypeDef     attributeTypeDef         = null;
    private TypeDef              typeDef                  = null;
    private TypeDefPatch         typeDefPatch             = null;
    private TypeDefSummary       originalTypeDefSummary   = null;
    private AttributeTypeDef     originalAttributeTypeDef = null;

    public OMRSEventV1TypeDefSection()
    {
    }

    public OMRSTypeDefEventType getTypeDefEventType()
    {
        return typeDefEventType;
    }

    public void setTypeDefEventType(OMRSTypeDefEventType typeDefEventType)
    {
        this.typeDefEventType = typeDefEventType;
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

    public AttributeTypeDef getAttributeTypeDef()
    {
        return attributeTypeDef;
    }

    public void setAttributeTypeDef(AttributeTypeDef attributeTypeDef)
    {
        this.attributeTypeDef = attributeTypeDef;
    }

    public TypeDef getTypeDef()
    {
        return typeDef;
    }

    public void setTypeDef(TypeDef typeDef)
    {
        this.typeDef = typeDef;
    }

    public TypeDefPatch getTypeDefPatch()
    {
        return typeDefPatch;
    }

    public void setTypeDefPatch(TypeDefPatch typeDefPatch)
    {
        this.typeDefPatch = typeDefPatch;
    }

    public TypeDefSummary getOriginalTypeDefSummary()
    {
        return originalTypeDefSummary;
    }

    public void setOriginalTypeDefSummary(TypeDefSummary originalTypeDefSummary)
    {
        this.originalTypeDefSummary = originalTypeDefSummary;
    }

    public AttributeTypeDef getOriginalAttributeTypeDef()
    {
        return originalAttributeTypeDef;
    }

    public void setOriginalAttributeTypeDef(AttributeTypeDef originalAttributeTypeDef)
    {
        this.originalAttributeTypeDef = originalAttributeTypeDef;
    }
}
