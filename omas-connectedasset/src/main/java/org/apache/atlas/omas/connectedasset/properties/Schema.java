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

package org.apache.atlas.omas.connectedasset.properties;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

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
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Schema extends SchemaElement
{
    public enum  SchemaType{ UNKNOWN, STRUCT, ARRAY, SET};

    /*
     * Properties specific to a Schema
     */
    SchemaType            schemaType       = SchemaType.UNKNOWN;
    List<SchemaAttribute> schemaAttributes = null;
    int                   maximumElements  = 0;
    List<SchemaLink>      schemaLinks      = null;


    /**
     * Default constructor
     */
    public Schema()
    {
        super();
    }


    /**
     * Copy/clone Constructor - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the definitions clone to point to the
     * asset clone and not the original asset.
     *
     * @param templateSchema - template object to copy.
     */
    public Schema(Schema templateSchema)
    {
        super(templateSchema);

        if (templateSchema != null)
        {
            List<SchemaAttribute>          templateSchemaAttributes = templateSchema.getSchemaAttributes();

            if (templateSchemaAttributes != null)
            {
                schemaAttributes = new ArrayList<>(templateSchemaAttributes);
            }
            schemaType  = templateSchema.getSchemaType();
            maximumElements = templateSchema.getMaximumElements();

            List<SchemaLink>     templateSchemaLinks = templateSchema.getSchemaLinks();

            if (templateSchemaLinks != null)
            {
                schemaLinks = new ArrayList<>(templateSchemaLinks);
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
     * Set up the type of the schema.
     *
     * @param schemaType - Struct, Array or Set
     */
    public void setSchemaType(SchemaType schemaType) { this.schemaType = schemaType; }


    /**
     * Return the list of schema attributes in this schema.
     *
     * @return SchemaAttributes
     */
    public List<SchemaAttribute> getSchemaAttributes()
    {
        if (schemaAttributes == null)
        {
            return schemaAttributes;
        }
        else
        {
            return new ArrayList<>(schemaAttributes);
        }
    }


    /**
     * Set up the list of schema attributes in this schema.
     *
     * @param schemaAttributes - list of attributes
     */
    public void setSchemaAttributes(List<SchemaAttribute> schemaAttributes) { this.schemaAttributes = schemaAttributes; }


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
            maximumElements = schemaAttributes.size();
        }

        return maximumElements;
    }


    /**
     * Set up the maximum elements that can be stored in this schema.  This is set up by the caller.
     * Zero means not bounded.  For a STRUCT the max elements are the number of elements in
     * the structure.
     *
     * @param maximumElements - int maximum number of elements
     */
    public void setMaximumElements(int maximumElements) { this.maximumElements = maximumElements; }


    /**
     * Return a list of any links that exist between the schema attributes of this schema (or others).
     * These links are typically used for network type schemas such as a grpah schema - or may be used to show
     * linkage to an element in another schema.
     *
     * @return SchemaLinks - list of linked schema attributes
     */
    public List<SchemaLink> getSchemaLinks()
    {
        if (schemaLinks == null)
        {
            return schemaLinks;
        }
        else
        {
            return new ArrayList<>(schemaLinks);
        }
    }


    /**
     * Set up a list of any links that exist between the schema attributes of this schema (or others).
     * These links are typically used for network type schemas such as a graph schema - or may be used to show
     * linkage to an element in another schema.
     *
     * @param schemaLinks - list of linked schema attributes
     */
    public void setSchemaLinks(List<SchemaLink> schemaLinks) { this.schemaLinks = schemaLinks; }

    /**
     * Returns a clone of this object as the abstract SchemaElement class.
     *
     * @return a copy of this schema as a SchemaElement
     */
    @Override
    public SchemaElement cloneSchemaElement()
    {
        return new Schema(this);
    }
}