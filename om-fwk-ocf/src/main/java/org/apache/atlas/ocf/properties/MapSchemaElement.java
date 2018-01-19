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
 * MapSchemaElement describes a schema element of type map.  It stores the type of schema element for the domain
 * (eg property name) for the map and the schema element for the range (eg property value) for the map.
 */
public class MapSchemaElement extends SchemaElement
{
    private   SchemaElement  mapFromElement = null;
    private   SchemaElement  mapToElement = null;


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
     * @param mapFromElement - the type of schema element that represents the key or property name for the map.
     *                          This is also called the domain of the map.
     * @param mapToElement - the type of schema element that represents the property value for the map.
     *                       This is also called the range of the map.
     */
    public MapSchemaElement(AssetDescriptor      parentAsset,
                            ElementType          type,
                            String               guid,
                            String               url,
                            Classifications      classifications,
                            String               qualifiedName,
                            AdditionalProperties additionalProperties,
                            Meanings             meanings,
                            String               versionNumber,
                            String               author,
                            String               usage,
                            String               encodingStandard,
                            SchemaElement        mapFromElement,
                            SchemaElement        mapToElement)
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

        this.mapFromElement = mapFromElement;
        this.mapToElement = mapToElement;
    }

    /**
     * Copy/clone Constructor - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the definitions clone to point to the
     * asset clone and not the original asset.
     *
     * @param parentAsset - description of the asset that this map is attached to.
     * @param templateSchema - template object to copy.
     */
    public MapSchemaElement(AssetDescriptor  parentAsset, MapSchemaElement templateSchema)
    {
        super(parentAsset, templateSchema);

        if (templateSchema != null)
        {
            SchemaElement  templateMapFromElement = templateSchema.getMapFromElement();
            SchemaElement  templateMapToElement = templateSchema.getMapToElement();

            if (templateMapFromElement != null)
            {
                mapFromElement = templateMapFromElement.cloneSchemaElement(super.getParentAsset());
            }

            if (templateMapToElement != null)
            {
                mapToElement = templateMapToElement.cloneSchemaElement(super.getParentAsset());
            }
        }
    }


    /**
     * Return the type of schema element that represents the key or property name for the map.
     * This is also called the domain of the map.
     *
     * @return SchemaElement
     */
    public SchemaElement getMapFromElement()
    {
        return mapFromElement;
    }


    /**
     * Return the type of schema element that represents the property value for the map.
     * This is also called the range of the map.
     *
     * @return SchemaElement
     */
    public SchemaElement getMapToElement()
    {
        return mapToElement;
    }


    /**
     * Returns a clone of this object as the abstract SchemaElement class.
     *
     * @param parentAsset - description of the asset that this schema element is attached to.
     * @return SchemaElement
     */
    @Override
    public SchemaElement cloneSchemaElement(AssetDescriptor parentAsset)
    {
        return new MapSchemaElement(parentAsset, this);
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "MapSchemaElement{" +
                "mapFromElement=" + mapFromElement +
                ", mapToElement=" + mapToElement +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}