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

package org.apache.atlas.ocf.properties;


/**
 * The Schema object provides information about how the asset structures the data it supports.  Schemas are typically
 * described as nested structures of linked schema elements.  Schemas can also be reused in other schemas.
 *
 * The schema object can be used to represent a Struct, Array, Set or Map.
 * <ul>
 *     <li>
 *         A Struct has an ordered list of attributes - the position of an attribute is set up as one of its properties.
 *     </li>
 *     <li>
 *         An Array has one schema attribute and a maximum size plus element count.
 *     </li>
 *     <li>
 *         A Set also has one schema attribute and a maximum size plus element count.
 *     </li>
 *     <li>
 *         A Map is a Set of MapSchemaElements
 *     </li>
 * </ul>
 */
public class Schema extends SchemaElement
{
    /*
     * Properties specific to a Schema
     */
    SchemaType                schemaType  = SchemaType.UNKNOWN;
    SchemaAttributes          schemaAttributes = null;
    int                       maximumElements = 0;
    SchemaLinks               schemaLinks = null;


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
     * @param schemaType - Struct, Array or Set
     * @param schemaAttributes - the list of schema attributes in this schema.
     * @param maximumElements - the maximum elements that can be stored in this schema.  This is set up by the caller.
     * Zero means not bounded.  For a STRUCT the max elements are the number of elements in
     * the structure.
     * @param schemaLinks - a list of any links that exist between the schema attributes of this schema (or others).
     * These links are typically used for network type schemas such as a graph schema - or may be used to show
     * linkage to an element in another schema.
     */
    public Schema(AssetDescriptor      parentAsset,
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
                  SchemaType           schemaType,
                  SchemaAttributes     schemaAttributes,
                  int                  maximumElements,
                  SchemaLinks          schemaLinks)
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

        this.schemaType = schemaType;
        this.schemaAttributes = schemaAttributes;
        this.maximumElements = maximumElements;
        this.schemaLinks = schemaLinks;
    }

    /**
     * Copy/clone Constructor - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the definitions clone to point to the
     * asset clone and not the original asset.
     *
     * @param parentAsset - description of the asset that this schema is attached to.
     * @param templateSchema - template object to copy.
     */
    public Schema(AssetDescriptor  parentAsset, Schema templateSchema)
    {
        super(parentAsset, templateSchema);

        if (templateSchema != null)
        {
            SchemaAttributes          templateSchemaAttributes = templateSchema.getSchemaAttributes();

            if (templateSchemaAttributes != null)
            {
                schemaAttributes = templateSchemaAttributes.cloneIterator(parentAsset);
            }
            schemaType  = templateSchema.getSchemaType();
            maximumElements = templateSchema.getMaximumElements();

            SchemaLinks              templateSchemaLinks = templateSchema.getSchemaLinks();

            if (templateSchemaLinks != null)
            {
                schemaLinks = templateSchemaLinks.cloneIterator(parentAsset);
            }
        }
    }


    /**
     * Return the type of the schema.
     *
     * @return SchemaType
     */
    public SchemaType getSchemaType() { return schemaType; }


    /**
     * Return the list of schema attributes in this schema.
     *
     * @return SchemaAttributes
     */
    public SchemaAttributes getSchemaAttributes()
    {
        if (schemaAttributes == null)
        {
            return schemaAttributes;
        }
        else
        {
            return schemaAttributes.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Return the maximum elements that can be stored in this schema.  This is set up by the caller.
     * Zero means not bounded.  For a STRUCT the max elements are the number of elements in
     * the structure.
     *
     * @return int maximum number of elements
     */
    public int getMaximumElements()
    {
        if (schemaType == SchemaType.STRUCT)
        {
            maximumElements = schemaAttributes.getElementCount();
        }

        return maximumElements;
    }


    /**
     * Return a list of any links that exist between the schema attributes of this schema (or others).
     * These links are typically used for network type schemas such as a grpah schema - or may be used to show
     * linkage to an element in another schema.
     *
     * @return SchemaLinks - list of linked schema attributes
     */
    public SchemaLinks getSchemaLinks()
    {
        if (schemaLinks == null)
        {
            return schemaLinks;
        }
        else
        {
            return schemaLinks.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Returns a clone of this object as the abstract SchemaElement class.
     *
     * @param parentAsset - description of the asset that this schema element is attached to.
     * @return a copy of this schema as a SchemaElement
     */
    @Override
    public SchemaElement cloneSchemaElement(AssetDescriptor parentAsset)
    {
        return new Schema(parentAsset, this);
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Schema{" +
                "schemaType=" + schemaType +
                ", schemaAttributes=" + schemaAttributes +
                ", maximumElements=" + maximumElements +
                ", schemaLinks=" + schemaLinks +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}