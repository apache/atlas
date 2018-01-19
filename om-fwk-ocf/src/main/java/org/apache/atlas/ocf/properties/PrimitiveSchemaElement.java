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


/**
 * PrimitiveSchemaElement describes a schema element that has a primitive type.  This class stores which
 * type of primitive type it is an a default value if supplied.
 */
public class PrimitiveSchemaElement extends SchemaElement
{
    private  String     dataType = null;
    private  String     defaultValue = null;


    /**
     * Typical constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param versionNumber - the version number of the schema element - null means no version number.
     * @param author - the name of the author of the schema element. Null means the author is unknown.
     * @param usage - the usage guidance for this schema element.  Null means no guidance available.
     * @param encodingStandard - encoding standard used for this schema.  It may be XML, JSON, SQL DDL or something else.
     *                           Null means the encoding standard is unknown or there are many choices.
     * @param dataType - the name of the data type for this element.  Null means unknown data type.
     * @param defaultValue - String containing default value for the element
     */
    public PrimitiveSchemaElement(AssetDescriptor parentAsset,
                                  ElementType type,
                                  String guid,
                                  String url,
                                  Classifications classifications,
                                  String qualifiedName,
                                  AdditionalProperties additionalProperties,
                                  Meanings meanings,
                                  String versionNumber,
                                  String author,
                                  String usage,
                                  String encodingStandard,
                                  String dataType,
                                  String defaultValue)
    {
        super(parentAsset,
              type,
              guid,
              url,
              classifications,
              qualifiedName,
              additionalProperties,
              meanings,
              versionNumber,
              author,
              usage,
              encodingStandard);

        this.dataType = dataType;
        this.defaultValue = defaultValue;
    }

    /**
     * Copy/clone Constructor - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the definitions clone to point to the
     * asset clone and not the original asset.
     *
     * @param parentAsset - description of the asset that this schema element is attached to.
     * @param templateSchemaElement - template object to copy.
     */
    public PrimitiveSchemaElement(AssetDescriptor  parentAsset, PrimitiveSchemaElement templateSchemaElement)
    {
        super(parentAsset, templateSchemaElement);

        if (templateSchemaElement != null)
        {
            dataType = templateSchemaElement.getDataType();
            defaultValue = templateSchemaElement.getDefaultValue();
        }
    }


    /**
     * Return the data type for this element.  Null means unknown data type.
     *
     * @return String DataType
     */
    public String getDataType() { return dataType; }


    /**
     * Return the default value for the element.  Null means no default value set up.
     *
     * @return String containing default value
     */
    public String getDefaultValue() { return defaultValue; }


    /**
     * Returns a clone of this object as the abstract SchemaElement class.
     *
     * @param parentAsset - description of the asset that this schema element is attached to.
     * @return PrimitiveSchemaElement object
     */
    @Override
    public SchemaElement cloneSchemaElement(AssetDescriptor parentAsset)
    {
        return new PrimitiveSchemaElement(parentAsset, this);
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "PrimitiveSchemaElement{" +
                "dataType='" + dataType + '\'' +
                ", defaultValue='" + defaultValue + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}