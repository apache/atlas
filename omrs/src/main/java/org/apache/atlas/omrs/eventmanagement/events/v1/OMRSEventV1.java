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

import org.apache.atlas.omrs.eventmanagement.events.OMRSEventCategory;
import org.apache.atlas.omrs.eventmanagement.events.OMRSEventOriginator;


import java.io.Serializable;
import java.util.Date;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;


/**
 * OMRSEventV1 is the OMRSEvent payload for version 1 of the open metadata and governance message exchange.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OMRSEventV1 implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final String                     protocolVersionId    = "OMRS V1.0";
    private       Date                       timestamp            = null;
    private       OMRSEventOriginator        originator           = null;
    private       OMRSEventCategory          eventCategory        = null;
    private       OMRSEventV1RegistrySection registryEventSection = null;
    private       OMRSEventV1TypeDefSection  typeDefEventSection  = null;
    private       OMRSEventV1InstanceSection instanceEventSection = null;
    private       OMRSEventV1ErrorSection    errorSection         = null;

    public OMRSEventV1()
    {
    }

    public String getProtocolVersionId()
    {
        return protocolVersionId;
    }

    public Date getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(Date timestamp)
    {
        this.timestamp = timestamp;
    }

    public OMRSEventOriginator getOriginator()
    {
        return originator;
    }

    public void setOriginator(OMRSEventOriginator originator)
    {
        this.originator = originator;
    }

    public OMRSEventCategory getEventCategory()
    {
        return eventCategory;
    }

    public void setEventCategory(OMRSEventCategory eventCategory)
    {
        this.eventCategory = eventCategory;
    }

    public OMRSEventV1RegistrySection getRegistryEventSection()
    {
        return registryEventSection;
    }

    public void setRegistryEventSection(OMRSEventV1RegistrySection registryEventSection)
    {
        this.registryEventSection = registryEventSection;
    }

    public OMRSEventV1TypeDefSection getTypeDefEventSection()
    {
        return typeDefEventSection;
    }

    public void setTypeDefEventSection(OMRSEventV1TypeDefSection typeDefEventSection)
    {
        this.typeDefEventSection = typeDefEventSection;
    }

    public OMRSEventV1InstanceSection getInstanceEventSection()
    {
        return instanceEventSection;
    }

    public void setInstanceEventSection(OMRSEventV1InstanceSection instanceEventSection)
    {
        this.instanceEventSection = instanceEventSection;
    }

    public OMRSEventV1ErrorSection getErrorSection()
    {
        return errorSection;
    }

    public void setErrorSection(OMRSEventV1ErrorSection errorSection)
    {
        this.errorSection = errorSection;
    }
}