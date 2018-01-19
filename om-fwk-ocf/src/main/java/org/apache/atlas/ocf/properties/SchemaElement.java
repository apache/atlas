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
public abstract class SchemaElement extends Referenceable
{
    private String              versionNumber = null;
    private String              author = null;
    private String              usage = null;
    private String              encodingStandard = null;


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
     */
    public SchemaElement(AssetDescriptor      parentAsset,
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
                         String               encodingStandard)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.versionNumber = versionNumber;
        this.author = author;
        this.usage = usage;
        this.encodingStandard = encodingStandard;
    }


    /**
     * Copy/clone Constructor - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the definitions clone to point to the
     * asset clone and not the original asset.
     *
     * @param parentAsset - description of the asset that this schema element is attached to.
     * @param templateSchema - template object to copy.
     */
    public SchemaElement(AssetDescriptor  parentAsset, SchemaElement templateSchema)
    {
        super(parentAsset, templateSchema);

        if (templateSchema != null)
        {
            versionNumber = templateSchema.getVersionNumber();
            author = templateSchema.getAuthor();
            usage = templateSchema.getUsage();
            encodingStandard = templateSchema.getEncodingStandard();
        }
    }


    /**
     * Return a clone of this schema element.  This method is needed because schema element
     * is abstract.
     *
     * @param parentAsset - description of the asset that this schema element is attached to.
     * @return Either a Schema or a PrimitiveSchemaElement depending on the type of the template.
     */
    public abstract SchemaElement   cloneSchemaElement(AssetDescriptor  parentAsset);


    /**
     * Return the version number of the schema element - null means no version number.
     *
     * @return String version number
     */
    public String getVersionNumber() { return versionNumber; }


    /**
     * Return the name of the author of the schema element.  Null means the author is unknown.
     *
     * @return String author name
     */
    public String getAuthor() { return author; }


    /**
     * Return the usage guidance for this schema element. Null means no guidance available.
     *
     * @return String usage guidance
     */
    public String getUsage() { return usage; }


    /**
     * Return the format (encoding standard) used for this schema.  It may be XML, JSON, SQL DDL or something else.
     * Null means the encoding standard is unknown or there are many choices.
     *
     * @return String encoding standard
     */
    public String getEncodingStandard() { return encodingStandard; }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "SchemaElement{" +
                "versionNumber='" + versionNumber + '\'' +
                ", author='" + author + '\'' +
                ", usage='" + usage + '\'' +
                ", encodingStandard='" + encodingStandard + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}