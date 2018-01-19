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
 * Derived schema elements are used in views to define elements that are calculated using data from other sources.
 * It contains a list of queries and a formula to combine the resulting values.
 */
public class DerivedSchemaElement extends PrimitiveSchemaElement
{
    private String                        formula = null;
    private SchemaImplementationQueries   queries = null;

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
     * @param formula - the formula used to combine the values of the queries.  Each query is numbers 0, 1, ... and the
     *                  formula has placeholders in it to show how the query results are combined.
     * @param queries - list of queries that are used to create the derived schema element.
     */
    public DerivedSchemaElement(AssetDescriptor parentAsset,
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
                                String defaultValue,
                                String formula,
                                SchemaImplementationQueries queries)
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
              encodingStandard,
              dataType,
              defaultValue);

        this.formula = formula;
        this.queries = queries;
    }

    /**
     * Copy/clone Constructor - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the definitions clone to point to the
     * asset clone and not the original asset.
     *
     * @param parentAsset - description of the asset that this schema element is attached to.
     * @param templateSchemaElement - template object to copy.
     */
    public DerivedSchemaElement(AssetDescriptor  parentAsset, DerivedSchemaElement templateSchemaElement)
    {
        super(parentAsset, templateSchemaElement);

        if (templateSchemaElement != null)
        {
            SchemaImplementationQueries   templateQueries = templateSchemaElement.getQueries();

            formula = templateSchemaElement.getFormula();
            queries = templateQueries.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Return the formula used to combine the values of the queries.  Each query is numbers 0, 1, ... and the
     * formula has placeholders in it to show how the query results are combined.
     *
     * @return String formula
     */
    public String getFormula() { return formula; }


    /**
     * Return the list of queries that are used to create the derived schema element.
     *
     * @return SchemaImplementationQueries - list of queries
     */
    public SchemaImplementationQueries getQueries()
    {
        if (queries == null)
        {
            return queries;
        }
        else
        {
            return queries.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Returns a clone of this object as the abstract SchemaElement class.
     *
     * @param parentAsset - description of the asset that this schema element is attached to.
     * @return PrimitiveSchemaElement object
     */
    @Override
    public SchemaElement cloneSchemaElement(AssetDescriptor parentAsset)
    {
        return new DerivedSchemaElement(parentAsset, this);
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "DerivedSchemaElement{" +
                "formula='" + formula + '\'' +
                ", queries=" + queries +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}