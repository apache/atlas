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
 * SchemaImplementationQuery defines a query on a schema attribute that returns all or part of the value for a
 * derived field.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class SchemaImplementationQuery extends PropertyBase
{
    private   int              queryId = 0;
    private   String           query = null;
    private   String           queryType = null;
    private   SchemaAttribute  queryTargetElement = null;

    /**
     * Typical Constructor - sets attributes to null.
     */
    public SchemaImplementationQuery()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param template - template schema query to copy.
     */
    public SchemaImplementationQuery(SchemaImplementationQuery   template)
    {
        super(template);

        if (template != null)
        {
            queryId = template.getQueryId();
            query = template.getQuery();
            queryType = template.getQueryType();

            SchemaAttribute    templateQueryTargetElement = template.getQueryTargetElement();
            if (templateQueryTargetElement != null)
            {
                queryTargetElement = new SchemaAttribute(templateQueryTargetElement);
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
     * Set up the query id - this is used to identify where the results of this query should be plugged into
     * the other queries or the formula for the parent derived schema element.
     *
     * @param queryId - int query identifier
     */
    public void setQueryId(int queryId) { this.queryId = queryId; }


    /**
     * Return the query string for this element.  The query string may have placeholders for values returned
     * by queries that have a lower queryId than this element.
     *
     * @return String query
     */
    public String getQuery() { return query; }


    /**
     * Set up the query string for this element.  The query string may have placeholders for values returned
     * by queries that have a lower queryId than this element.
     *
     * @param query - String with placeholders
     */
    public void setQuery(String query) { this.query = query; }


    /**
     * Return the name of the query language used in the query.
     *
     * @return queryType String
     */
    public String getQueryType() { return queryType; }


    /**
     * Set up the name of the query language used in the query.
     *
     * @param queryType String
     */
    public void setQueryType(String queryType) { this.queryType = queryType; }

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
            return new SchemaAttribute(queryTargetElement);
        }
    }


    /**
     * Set up the SchemaAttribute that describes the type of the data source that will be queried to get the
     * derived value.
     *
     * @param queryTargetElement - SchemaAttribute
     */
    public void setQueryTargetElement(SchemaAttribute queryTargetElement)
    {
        this.queryTargetElement = queryTargetElement;
    }
}