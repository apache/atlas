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
package org.apache.atlas.omrs.metadatacollection.properties.typedefs;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * TypeDefAttribute stores the properties used to describe a attribute within a Classification,
 * Entity or Relationship.  The attribute may itself be of types Enum, Collection or Primitive Types.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TypeDefAttribute extends TypeDefElementHeader
{
    protected String                             attributeName            = null;
    protected AttributeTypeDef                   attributeType            = null;
    protected String                             attributeDescription     = null;
    protected String                             attributeDescriptionGUID = null;
    protected AttributeCardinality               cardinality              = AttributeCardinality.UNKNOWN;
    protected int                                valuesMinCount           = 0;
    protected int                                valuesMaxCount           = 1;
    protected boolean                            isIndexable              = true;
    protected boolean                            isUnique                 = false;
    protected String                             defaultValue             = null;
    protected ArrayList<ExternalStandardMapping> externalStandardMappings = null;


    /**
     * Default constructor creates an empty TypeDefAttribute.
     */
    public TypeDefAttribute()
    {
        super();
    }


    /**
     * Copy/clone constructor initialized with the values from the supplied template.
     *
     * @param template - TypeDefAttribute to copy
     */
    public TypeDefAttribute(TypeDefAttribute template)
    {
        super(template);

        if (template != null)
        {
            attributeName = template.getAttributeName();
            attributeType = template.getAttributeType();
            attributeDescription = template.getAttributeDescription();
            attributeDescriptionGUID = template.getAttributeDescriptionGUID();
            cardinality = template.getAttributeCardinality();
            valuesMinCount = template.getValuesMinCount();
            valuesMaxCount = template.getValuesMaxCount();
            isUnique = template.isUnique();
            isIndexable = template.isIndexable();
            defaultValue = template.getDefaultValue();
            this.setExternalStandardMappings(template.getExternalStandardMappings());
        }
    }


    /**
     * Return the name of this attribute.
     *
     * @return String name
     */
    public String getAttributeName()
    {
        return attributeName;
    }


    /**
     * Set up the name of this attribute.
     *
     * @param attributeName - String name
     */
    public void setAttributeName(String attributeName)
    {
        this.attributeName = attributeName;
    }


    /**
     * Return the name of the type for the value in this attribute.
     *
     * @return AttributeTypeDef - definition of attribute type
     */
    public AttributeTypeDef getAttributeType() { return attributeType; }


    /**
     * Set up the name of the type for the value in this attribute.
     *
     * @param attributeType AttributeTypeDef
     */
    public void setAttributeType(AttributeTypeDef attributeType) { this.attributeType = attributeType; }


    /**
     * Return the short description of the attribute.
     *
     * @return String description
     */
    public String getAttributeDescription()
    {
        return attributeDescription;
    }


    /**
     * Set up the short description of the attribute.
     *
     * @param attributeDescription - String description
     */
    public void setAttributeDescription(String attributeDescription)
    {
        this.attributeDescription = attributeDescription;
    }


    /**
     * Return the unique id of the glossary term that describes this attribute (or null if
     * no attribute defined).
     *
     * @return String guid
     */
    public String getAttributeDescriptionGUID()
    {
        return attributeDescriptionGUID;
    }


    /**
     * Set up the unique id of the glossary term that describes this attribute (or null if
     * no attribute defined).
     *
     * @param attributeDescriptionGUID - String guid
     */
    public void setAttributeDescriptionGUID(String attributeDescriptionGUID)
    {
        this.attributeDescriptionGUID = attributeDescriptionGUID;
    }


    /**
     * Return the cardinality of this attribute.
     *
     * @return AttributeCardinality Enum.
     */
    public AttributeCardinality getAttributeCardinality() { return cardinality; }


    /**
     * Set up the cardinality for this attribute.
     *
     * @param attributeCardinality enum value
     */
    public void setAttributeCardinality(AttributeCardinality attributeCardinality) { this.cardinality = attributeCardinality; }


    /**
     * Return the minimum number of values for this attribute (relevant for Arrays, Sets and Maps).
     *
     * @return int minimum count
     */
    public int getValuesMinCount() { return valuesMinCount; }


    /**
     * Set up the minimum number of values for this attribute (relevant for Arrays, Sets and Maps).
     *
     * @param valuesMinCount int minimum count
     */
    public void setValuesMinCount(int valuesMinCount) { this.valuesMinCount = valuesMinCount; }


    /**
     * Return the maximum number of values for this attribute (relevant for Arrays, Sets and Maps).
     *
     * @return int maximum count
     */
    public int getValuesMaxCount() { return valuesMaxCount; }


    /**
     * Set up the maximum number of values for this attribute (relevant for Arrays, Sets and Maps).
     *
     * @param valuesMaxCount int maximum count
     */
    public void setValuesMaxCount(int valuesMaxCount) { this.valuesMaxCount = valuesMaxCount; }


    /**
     * Return whether the value for this attribute is unique across the specific instances of Struct, Classification,
     * Entity or Relationship types that this attribute included in.
     *
     * @return boolean isUnique flag
     */
    public boolean isUnique() { return isUnique; }


    /**
     * Set up the isUnique flag.  This indicates whether the value for this attribute is unique
     * across the specific instances of Struct, Classification,
     * Entity or Relationship types that this attribute included in.
     *
     * @param unique boolean isUnique flag
     */
    public void setUnique(boolean unique) { isUnique = unique; }


    /**
     * Return whether this attribute should be included in the metadata collection's search index.
     *
     * @return boolean isIndexable flag
     */
    public boolean isIndexable() { return isIndexable; }


    /**
     * Set up the isIndexable flag.  This indicates whether this attribute should be included in the
     * metadata collection's search index.
     *
     * @param indexable boolean isIndexable flag
     */
    public void setIndexable(boolean indexable) { isIndexable = indexable; }


    /**
     * Return the default value for this attribute.
     *
     * @return String default value
     */
    public String getDefaultValue() { return defaultValue; }


    /**
     * Set up the default value for this attribute.
     *
     * @param defaultValue String
     */
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }


    /**
     * Return the list of mappings to external standards.
     *
     * @return ExternalStandardMappings list
     */
    public List<ExternalStandardMapping> getExternalStandardMappings()
    {
        if (externalStandardMappings == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(externalStandardMappings);
        }
    }


    /**
     * Set up the list of mappings to external standards.
     *
     * @param externalStandardMappings - ExternalStandardMappings list
     */
    public void setExternalStandardMappings(List<ExternalStandardMapping> externalStandardMappings)
    {
        if (externalStandardMappings == null)
        {
            this.externalStandardMappings = null;
        }
        else
        {
            this.externalStandardMappings = new ArrayList<>(externalStandardMappings);
        }
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "TypeDefAttribute{" +
                "attributeName='" + attributeName + '\'' +
                ", attributeType=" + attributeType +
                ", cardinality=" + cardinality +
                ", valuesMinCount=" + valuesMinCount +
                ", valuesMaxCount=" + valuesMaxCount +
                ", isIndexable=" + isIndexable +
                ", isUnique=" + isUnique +
                ", defaultValue='" + defaultValue + '\'' +
                ", externalStandardMappings=" + externalStandardMappings +
                '}';
    }
}
