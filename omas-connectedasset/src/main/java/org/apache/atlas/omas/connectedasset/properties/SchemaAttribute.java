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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * <p>
 *     SchemaAttribute describes a single attribute within a schema.  The attribute has a name, order in the
 *     schema and cardinality.
 *     Its type is another SchemaElement (either Schema or PrimitiveSchemaElement).
 * </p>
 * <p>
 *     If it is a PrimitiveSchemaElement it may have an override for the default value within.
 * </p>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class SchemaAttribute extends PropertyBase
{
    String        attributeName        = null;
    int           elementPosition      = 0;
    String        cardinality          = null;
    String        defaultValueOverride = null;
    SchemaElement attributeType        = null;


    /**
     * Default Constructor - sets attribute to null.
     */
    public SchemaAttribute()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param template - template schema attribute to copy.
     */
    public SchemaAttribute(SchemaAttribute   template)
    {
        super(template);

        if (template != null)
        {
            attributeName = template.getAttributeName();
            elementPosition = template.getElementPosition();
            cardinality = template.getCardinality();
            defaultValueOverride = template.getDefaultValueOverride();

            SchemaElement  templateAttributeType = template.getAttributeType();
            if (templateAttributeType != null)
            {
                /*
                 * SchemaElement is an abstract class with a placeholder method to clone an object
                 * of its sub-class.  When cloneSchemaElement() is called, the implementation in the
                 * sub-class is called.
                 */
                attributeType = templateAttributeType.cloneSchemaElement();
            }
        }
    }


    /**
     * Return the name of this schema attribute.
     *
     * @return String attribute name
     */
    public String getAttributeName() { return attributeName; }


    /**
     * Set up the name of this attribute.
     *
     * @param attributeName - String
     */
    public void setAttributeName(String attributeName) { this.attributeName = attributeName; }


    /**
     * Return the position of this schema attribute in its parent schema.
     *
     * @return int position in schema - 0 means first
     */
    public int getElementPosition() { return elementPosition; }


    /**
     * Set up the position of this schema attribute in its parent schema.
     *
     * @param elementPosition - int position in schema - 0 means first
     */
    public void setElementPosition(int elementPosition) { this.elementPosition = elementPosition; }


    /**
     * Return the cardinality defined for this schema attribute.
     *
     * @return String cardinality defined for this schema attribute.
     */
    public String getCardinality() { return cardinality; }


    /**
     * Set up the cardinality defined for this schema attribute.
     *
     * @param cardinality - String cardinality defined for this schema attribute.
     */
    public void setCardinality(String cardinality) { this.cardinality = cardinality; }


    /**
     * Return any default value for this attribute that would override the default defined in the
     * schema element for this attribute's type (note only used is type is primitive).
     *
     * @return String default value override
     */
    public String getDefaultValueOverride() { return defaultValueOverride; }


    /**
     * Set up any default value for this attribute that would override the default defined in the
     * schema element for this attribute's type (note only used is type is primitive).
     *
     * @param defaultValueOverride - String default value override
     */
    public void setDefaultValueOverride(String defaultValueOverride)
    {
        this.defaultValueOverride = defaultValueOverride;
    }


    /**
     * Return the SchemaElement that relates to the type of this attribute.
     *
     * @return SchemaElement
     */
    public SchemaElement getAttributeType()
    {
        if (attributeType == null)
        {
            return attributeType;
        }
        else
        {
            return attributeType.cloneSchemaElement();
        }
    }


    /**
     * Set up the SchemaElement that relates to the type of this attribute.
     *
     * @param attributeType SchemaElement
     */
    public void setAttributeType(SchemaElement attributeType) { this.attributeType = attributeType; }
}