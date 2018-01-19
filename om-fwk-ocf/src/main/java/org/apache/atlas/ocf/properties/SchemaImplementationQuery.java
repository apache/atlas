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
 * SchemaImplementationQuery defines a query on a schema attribute that returns all or part of the value for a
 * derived field.
 */
public class SchemaImplementationQuery extends AssetPropertyBase
{
    private   int              queryId = 0;
    private   String           query = null;
    private   String           queryType = null;
    private   SchemaAttribute  queryTargetElement = null;


    /**
     * Typical Constructor - sets attributes to null.
     *
     * @param parentAsset - description of the asset that this schema query is attached to.
     * @param queryId - int query identifier - this is used to identify where the results of this query should be plugged into
     *                 the other queries or the formula for the parent derived schema element.
     * @param query - query string for this element.  The query string may have placeholders for values returned
     *                by queries that have a lower queryId than this element.
     * @param queryType - the name of the query language used in the query.
     * @param queryTargetElement - the SchemaAttribute that describes the type of the data source that
     *                           will be queried to get the derived value.
     */
    public SchemaImplementationQuery(AssetDescriptor parentAsset, int queryId, String query, String queryType, SchemaAttribute queryTargetElement)
    {
        super(parentAsset);

        this.queryId = queryId;
        this.query = query;
        this.queryType = queryType;
        this.queryTargetElement = queryTargetElement;
    }


    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - description of the asset that this schema query is attached to.
     * @param template - template schema query to copy.
     */
    public SchemaImplementationQuery(AssetDescriptor   parentAsset, SchemaImplementationQuery   template)
    {
        super(parentAsset, template);

        if (template != null)
        {
            queryId = template.getQueryId();
            query = template.getQuery();
            queryType = template.getQueryType();

            SchemaAttribute    templateQueryTargetElement = template.getQueryTargetElement();
            if (templateQueryTargetElement != null)
            {
                queryTargetElement = new SchemaAttribute(super.getParentAsset(), templateQueryTargetElement);
            }
        }
    }


    /**
     * Return the query id - this is used to identify where the results of this query should be plugged into
     * the other queries or the formula for the parent derived schema element.
     *
     * @return int query identifier
     */
    public int getQueryId() { return queryId; }


    /**
     * Return the query string for this element.  The query string may have placeholders for values returned
     * by queries that have a lower queryId than this element.
     *
     * @return String query
     */
    public String getQuery() { return query; }


    /**
     * Return the name of the query language used in the query.
     *
     * @return queryType String
     */
    public String getQueryType() { return queryType; }


    /**
     * Return the SchemaAttribute that describes the type of the data source that will be queried to get the
     * derived value.
     *
     * @return SchemaAttribute
     */
    public SchemaAttribute getQueryTargetElement()
    {
        if (queryTargetElement == null)
        {
            return queryTargetElement;
        }
        else
        {
            return new SchemaAttribute(super.getParentAsset(), queryTargetElement);
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
        return "SchemaImplementationQuery{" +
                "queryId=" + queryId +
                ", query='" + query + '\'' +
                ", queryType='" + queryType + '\'' +
                ", queryTargetElement=" + queryTargetElement +
                '}';
    }
}