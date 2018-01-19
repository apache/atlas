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
package org.apache.atlas.ocf.properties;


import java.util.ArrayList;

/**
 * SchemaLink defines a relationship between 2 SchemaElements.  It is used in network type schemas such as a graph.
 */
public class SchemaLink extends AssetPropertyBase
{
    /*
     * Attributes from the relationship
     */
    private String               linkGUID = null;
    private String               linkType = null;

    /*
     * Attributes specific to SchemaLink
     */
    private String               linkName             = null;
    private AdditionalProperties linkProperties       = null;
    private ArrayList<String>    linkedAttributeGUIDs = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor of parent asset
     * @param linkGUID - the identifier of the schema link.
     * @param linkType - the type of the link - this is related to the type of the schema it is a part of.
     * @param linkName - the name of the schema link.
     * @param linkProperties - the list of properties associated with this schema link.
     * @param linkedAttributeGUIDs - GUIDs for either end of the link - return as an iterator.
     */
    public SchemaLink(AssetDescriptor      parentAsset,
                      String               linkGUID,
                      String               linkType,
                      String               linkName,
                      AdditionalProperties linkProperties,
                      ArrayList<String>    linkedAttributeGUIDs)
    {
        super(parentAsset);

        this.linkGUID = linkGUID;
        this.linkType = linkType;
        this.linkName = linkName;
        this.linkProperties = linkProperties;
        this.linkedAttributeGUIDs = linkedAttributeGUIDs;
    }


    /**
     * Copy/clone constructor - makes a copy of the supplied object.
     *
     * @param parentAsset - descriptor of parent asset
     * @param template - template object to copy
     */
    public SchemaLink(AssetDescriptor parentAsset, SchemaLink template)
    {
        super(parentAsset, template);

        if (template != null)
        {
            linkGUID = template.getLinkGUID();
            linkName = template.getLinkName();
            linkType = template.getLinkType();

            AdditionalProperties   templateLinkProperties = template.getLinkProperties();
            if (templateLinkProperties != null)
            {
                linkProperties = new AdditionalProperties(super.getParentAsset(), templateLinkProperties);
            }

            linkedAttributeGUIDs = template.getLinkedAttributeGUIDs();
        }
    }


    /**
     * Return the identifier for the schema link.
     *
     * @return String guid
     */
    public String getLinkGUID() { return linkGUID; }


    /**
     * Return the type of the link - this is related to the type of the schema it is a part of.
     *
     * @return String link type
     */
    public String getLinkType() { return linkType; }


    /**
     * Return the name of this link
     *
     * @return String name
     */
    public String getLinkName() { return linkName; }


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
            return new AdditionalProperties(super.getParentAsset(), linkProperties);
        }
    }


    /**
     * Return the GUIDs of the schema attributes that this link connects together.
     *
     * @return SchemaAttributeGUIDs - GUIDs for either end of the link - return as a list.
     */
    public ArrayList<String> getLinkedAttributeGUIDs()
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
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "SchemaLink{" +
                "linkGUID='" + linkGUID + '\'' +
                ", linkType='" + linkType + '\'' +
                ", linkName='" + linkName + '\'' +
                ", linkProperties=" + linkProperties +
                ", linkedAttributeGUIDs=" + linkedAttributeGUIDs +
                '}';
    }
}