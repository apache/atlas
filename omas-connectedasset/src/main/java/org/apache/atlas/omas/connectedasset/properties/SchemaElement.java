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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * <p>
 *     The SchemaElement object provides a base class for the pieces that make up a schema for a data asset.
 *     A schema provides information about how the data is structured in the asset.  Schemas are typically
 *     described as nested structures of linked schema elements.  Schemas can also be reused in other schemas.
 * </p>
 *     SchemaElement is an abstract class - used to enable the most accurate and precise mapping of the
 *     elements in a schema to the asset.
 *     <ul>
 *         <li>PrimitiveSchemaElement is for a leaf element in a schema.</li>
 *         <li>Schema is a complex structure of nested schema elements.</li>
 *         <li>MapSchemaElement is for an attribute of type Map</li>
 *     </ul>
 *     Most assets will be linked to a Schema.
 * <p>
 *     Schema elements can be linked to one another using SchemaLink.
 * </p>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class SchemaElement extends Referenceable
{
    private String              versionNumber = null;
    private String              author = null;
    private String              usage = null;
    private String              encodingStandard = null;


    /**
     * Default constructor
     */
    public SchemaElement()
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
    public SchemaElement(SchemaElement templateSchema)
    {
        super(templateSchema);

        if (templateSchema != null)
        {
            versionNumber = templateSchema.getVersionNumber();
            author = templateSchema.getAuthor();
            usage = templateSchema.getUsage();
            encodingStandard = templateSchema.getEncodingStandard();
        }
    }


    /**
     * Return a clone of the this schema element.  This method is needed because schema element
     * is abstract.
     *
     * @return Either a Schema or a PrimitiveSchemaElement depending on the type of the template.
     */
    public abstract SchemaElement   cloneSchemaElement();


    /**
     * Return the version number of the schema element - null means no version number.
     *
     * @return String version number
     */
    public String getVersionNumber() { return versionNumber; }


    /**
     * Set up the version number of the schema element - null means no version number.
     *
     * @param versionNumber - String
     */
    public void setVersionNumber(String versionNumber) { this.versionNumber = versionNumber; }


    /**
     * Return the name of the author of the schema element.  Null means the author is unknown.
     *
     * @return String author name
     */
    public String getAuthor() { return author; }


    /**
     * Set up the name of the author of the schema element. Null means the author is unknown.
     *
     * @param author - String author name
     */
    public void setAuthor(String author) { this.author = author; }


    /**
     * Return the usage guidance for this schema element. Null means no guidance available.
     *
     * @return String usage guidance
     */
    public String getUsage() { return usage; }


    /**
     * Set up the usage guidance for this schema element.  Null means no guidance available.
     *
     * @param usage - String usage guidance
     */
    public void setUsage(String usage) { this.usage = usage; }


    /**
     * Return the format (encoding standard) used for this schema.  It may be XML, JSON, SQL DDL or something else.
     * Null means the encoding standard is unknown or there are many choices.
     *
     * @return String encoding standard
     */
    public String getEncodingStandard() { return encodingStandard; }


    /**
     * Set up the format (encoding standard) used for this schema.  It may be XML, JSON, SQL DDL or something else.
     * Null means the encoding standard is unknown or there are many choices.
     *
     * @param encodingStandard - String encoding standard
     */
    public void setEncodingStandard(String encodingStandard) { this.encodingStandard = encodingStandard; }
}