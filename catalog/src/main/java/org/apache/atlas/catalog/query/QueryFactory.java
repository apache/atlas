/**
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

package org.apache.atlas.catalog.query;

import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.TermPath;
import org.apache.atlas.catalog.definition.*;
import org.apache.atlas.catalog.exception.CatalogRuntimeException;
import org.apache.atlas.catalog.exception.InvalidQueryException;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.sandbox.queries.regex.RegexQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory used to create QueryAdapter instances.
 */
public class QueryFactory {
    private static final Logger LOG = LoggerFactory.getLogger(QueryFactory.class);
    public static final String PATH_SEP_TOKEN = "__slash__";

    private final Map<Class<? extends Query>, ExpressionCreateFunction<? extends Query>>
            expressionCreateFunctions = new HashMap<>();

    public QueryFactory() {
        registerExpressionCreateFunctions();
    }

    public AtlasQuery createTaxonomyQuery(Request request) throws InvalidQueryException {
        ResourceDefinition taxonomyDefinition = new TaxonomyResourceDefinition();
        QueryExpression queryExpression = create(request, taxonomyDefinition);
        return new AtlasTaxonomyQuery(queryExpression, taxonomyDefinition, request);
    }

    public AtlasQuery createTermQuery(Request request) throws InvalidQueryException {
        ResourceDefinition termDefinition = new TermResourceDefinition();
        QueryExpression queryExpression = create(request, termDefinition);
        TermPath termPath = request.getProperty("termPath");
        return new AtlasTermQuery(queryExpression, termDefinition, termPath, request);
    }

    public AtlasQuery createEntityQuery(Request request) throws InvalidQueryException {
        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        QueryExpression queryExpression = create(request, entityDefinition);
        return new AtlasEntityQuery(queryExpression, entityDefinition, request);
    }

    public AtlasQuery createEntityTagQuery(Request request) throws InvalidQueryException {
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        QueryExpression queryExpression = create(request, entityTagDefinition);
        String guid = request.getProperty("id");
        return new AtlasEntityTagQuery(queryExpression, entityTagDefinition, guid, request);
    }

    private QueryExpression create(Request request, ResourceDefinition resourceDefinition) throws InvalidQueryException {
        String queryString;
        if (request.getCardinality() == Request.Cardinality.INSTANCE) {
            String idPropertyName = resourceDefinition.getIdPropertyName();
            queryString = String.format("%s:%s", idPropertyName, request.<String>getProperty(idPropertyName));
        } else {
            queryString = request.getQueryString();
        }

        QueryExpression queryExpression;
        if (queryString != null && !queryString.isEmpty()) {
            QueryParser queryParser = new QueryParser(Version.LUCENE_48, "name", new KeywordAnalyzer());
            queryParser.setLowercaseExpandedTerms(false);
            Query query;
            try {
                query = queryParser.parse((String) escape(queryString));
            } catch (ParseException e) {
                throw new InvalidQueryException(e.getMessage());
            }
            LOG.info("LuceneQuery: " + query);
            queryExpression = create(query, resourceDefinition);
        } else {
            queryExpression = new AlwaysQueryExpression();
        }
        // add query properties to request so that they are returned
        request.addAdditionalSelectProperties(queryExpression.getProperties());
        return queryExpression;
    }

    @SuppressWarnings("unchecked")
    protected <T extends Query> QueryExpression create(T query, ResourceDefinition resourceDefinition) {
        if (! expressionCreateFunctions.containsKey(query.getClass())) {
            throw new CatalogRuntimeException("Query type currently not supported: " + query.getClass(), 400);
        }
        //todo: fix generic typing
        ExpressionCreateFunction expressionCreateFunction = expressionCreateFunctions.get(query.getClass());
        return expressionCreateFunction.createExpression(query, resourceDefinition);

    }

    // "escapes" characters as necessary for lucene parser
    //todo: currently '/' characters are blindly being replaced but this will not allow regex queries to be used
    protected static Object escape(Object val) {
        if (val instanceof String) {
            return ((String)val).replaceAll("/", PATH_SEP_TOKEN);
        } else {
            return val;
        }
    }

    private abstract static class ExpressionCreateFunction<T extends Query> {
        QueryExpression createExpression(T query, ResourceDefinition resourceDefinition) {
            QueryExpression expression = create(query, resourceDefinition);
            return expression.isProjectionExpression() ?
                    new ProjectionQueryExpression(expression, resourceDefinition) :
                    expression;
        }

        protected abstract QueryExpression create(T query, ResourceDefinition resourceDefinition);
    }

    private void registerExpressionCreateFunctions() {
        expressionCreateFunctions.put(WildcardQuery.class, new ExpressionCreateFunction<WildcardQuery>() {
            @Override
            public QueryExpression create(WildcardQuery query, ResourceDefinition definition) {
                return new WildcardQueryExpression(query, definition);
            }
        });

        expressionCreateFunctions.put(PrefixQuery.class, new ExpressionCreateFunction<PrefixQuery>() {
            @Override
            public QueryExpression create(PrefixQuery query, ResourceDefinition definition) {
                return new PrefixQueryExpression(query, definition);
            }
        });

        expressionCreateFunctions.put(TermQuery.class, new ExpressionCreateFunction<TermQuery>() {
            @Override
            public QueryExpression create(TermQuery query, ResourceDefinition definition) {
                return new TermQueryExpression(query, definition);
            }
        });

        expressionCreateFunctions.put(TermRangeQuery.class, new ExpressionCreateFunction<TermRangeQuery>() {
            @Override
            public QueryExpression create(TermRangeQuery query, ResourceDefinition definition) {
                return new TermRangeQueryExpression(query, definition);
            }
        });

        expressionCreateFunctions.put(RegexQuery.class, new ExpressionCreateFunction<RegexQuery>() {
            @Override
            public QueryExpression create(RegexQuery query, ResourceDefinition definition) {
                return new RegexQueryExpression(query, definition);
            }
        });

        expressionCreateFunctions.put(BooleanQuery.class, new ExpressionCreateFunction<BooleanQuery>() {
            @Override
            public QueryExpression create(BooleanQuery query, ResourceDefinition definition) {
                return new BooleanQueryExpression(query, definition, QueryFactory.this);
            }
        });
    }
}
