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
package org.apache.atlas.omas.connectedasset.properties;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * SchemaLink defines a relationship between 2 SchemaElements.  It is used in network type schemas such as a graph.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class SchemaLink extends PropertyBase
{
    /*
     * Attributes from the relationship
     */
    private String                 linkGUID = null;
    private String                 linkType = null;

    /*
     * Attributes specific to SchemaLink
     */
    private String               linkName             = null;
    private AdditionalProperties linkProperties       = null;
    private List<String>         linkedAttributeGUIDs = null;


    /**
     * Default Constructor
     */
    public SchemaLink()
    {
        super();
    }


    /**
     * Copy/clone constructor - makes a copy of the supplied object.
     *
     * @param template - template object to copy
     */
    public SchemaLink(SchemaLink template)
    {
        super(template);

        if (template != null)
        {
            linkGUID = template.getLinkGUID();
            linkName = template.getLinkName();

            AdditionalProperties   templateLinkProperties = template.getLinkProperties();
            if (templateLinkProperties != null)
            {
                linkProperties = new AdditionalProperties(templateLinkProperties);
            }

            List<String>  templateLinkedAttributeGUIDs = template.getLinkedAttributeGUIDs();
            if (templateLinkedAttributeGUIDs != null)
            {
                linkedAttributeGUIDs = new ArrayList<>(templateLinkedAttributeGUIDs);
            }
        }
    }


    /**
     * Return the identifier for the schema link.
     *
     * @return String guid
     */
    public String getLinkGUID() { return linkGUID; }


    /**
     * Set up the identifier of the schema link.
     *
     * @param linkGUID - String guid
     */
    public void setLinkGUID(String linkGUID) { this.linkGUID = linkGUID; }


    /**
     * Return the type of the link - this is related to the type of the schema it is a part of.
     *
     * @return String link type
     */
    public String getLinkType() { return linkType; }


    /**
     * Set up the type of the link - this is related to the type of the schema it is a part of.
     *
     * @param linkType - String link type
     */
    public void setLinkType(String linkType) { this.linkType = linkType; }

    /**
     * Return the name of this link
     *
     * @return String name
     */
    public String getLinkName() { return linkName; }


    /**
     * Set up the name of the schema link.
     *
     * @param linkName - String link name
     */
    public void setLinkName(String linkName) { this.linkName = linkName; }


    /**
     * Return the list of properties associated with this schema link.
     *
     * @return AdditionalProperties
     */
    public AdditionalProperties getLinkProperties()
    {
        if (linkProperties == null)
        {
            return linkProperties;
        }
        else
        {
            return new AdditionalProperties(linkProperties);
        }
    }


    /**
     * Set up the list of properties associated with this schema link.
     *
     * @param linkProperties - AdditionalProperties
     */
    public void setLinkProperties(AdditionalProperties linkProperties) { this.linkProperties = linkProperties; }


    /**
     * Return the GUIDs of the schema attributes that this link connects together.
     *
     * @return SchemaAttributeGUIDs - GUIDs for either end of the link - return as a list.
     */
    public List<String> getLinkedAttributeGUIDs()
    {
        if (linkedAttributeGUIDs == null)
        {
            return linkedAttributeGUIDs;
        }
        else
        {
            return new ArrayList<>(linkedAttributeGUIDs);
        }
    }


    /**
     * Set up the GUIDs of the schema attributes that this link connects together.
     *
     * @param linkedAttributeOneGUID - String GUID for a schema attribute
     * @param linkedAttributeTwoGUID - String GUID for a schema attribute
     */
    public void setLinkedAttributeGUIDs(String linkedAttributeOneGUID, String linkedAttributeTwoGUID)
    {
        List<String>    linkedAttributeArray = new ArrayList<>();

        linkedAttributeArray.add(linkedAttributeOneGUID);
        linkedAttributeArray.add(linkedAttributeTwoGUID);

        this.linkedAttributeGUIDs = linkedAttributeArray;
    }
}