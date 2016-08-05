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
package org.apache.atlas.repository.graphdb.titan.query;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.titan.query.expr.AndCondition;
import org.apache.atlas.repository.graphdb.titan.query.expr.HasPredicate;
import org.apache.atlas.repository.graphdb.titan.query.expr.InPredicate;
import org.apache.atlas.repository.graphdb.titan.query.expr.OrCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of AtlasGraphQuery that is used by both Titan 0.5.4
 * and Titan 1.0.0.
 *
 * Represents a graph query as an OrConditions which consists of
 * 1 or more AndConditions.  The query is executed by converting
 * the AndConditions to native GraphQuery instances that can be executed
 * directly against Titan.  The overall result is obtained by unioning together
 * the results from those individual GraphQueries.
 *
 * Here is a pictoral view of what is going on here.  Conceptually,
 * the query being executed can be though of as the where clause
 * in a query
 *
 *
 * where (a =1 and b=2) or (a=2 and b=3)
 *
 *                ||
 *               \||/
 *                \/
 *
 *           OrCondition
 *                 |
 *       +---------+--------+
 *       |                  |
 *   AndCondition     AndCondition
 *   (a=1 and b=2)     (a=2 and b=3)
 *
 *       ||                 ||
 *      \||/               \||/
 *       \/                 \/
 *
 *   GraphQuery          GraphQuery
 *
 *       ||                 ||
 *      \||/               \||/
 *       \/                 \/
 *
 *     vertices          vertices
 *           \            /
 *           _\/        \/_
 *               (UNION)
 *
 *                 ||
 *                \||/
 *                 \/
 *
 *               result
 *
 *
 *
 */
public abstract class TitanGraphQuery<V,E> implements AtlasGraphQuery<V, E> {

    private final Logger LOG = LoggerFactory.getLogger(TitanGraphQuery.class);
    protected final AtlasGraph<V,E> graph_;
    private final OrCondition queryCondition_ = new OrCondition();
    private final boolean isChildQuery_;
    protected abstract NativeTitanQueryFactory<V, E> getQueryFactory();

    /**
     * Creates a TitanGraphQuery
     *
     * @param graph
     */
    public TitanGraphQuery(AtlasGraph<V,E> graph) {
        graph_ = graph;
        isChildQuery_ = false;
    }

    /**
     * Creates a TitanGraphQuery
     *
     * @param graph
     * @param isChildQuery
     */
    public TitanGraphQuery(AtlasGraph<V,E> graph, boolean isChildQuery) {
        graph_ = graph;
        isChildQuery_ = isChildQuery;
    }

    @Override
    public AtlasGraphQuery<V, E> has(String propertyKey, Object value) {
        queryCondition_.andWith(new HasPredicate(propertyKey, ComparisionOperator.EQUAL, value));
        return this;
    }

    @Override
    public Iterable<AtlasVertex<V, E>> vertices() {
        LOG.debug("Executing: " );
        LOG.debug(queryCondition_.toString());
        //compute the overall result by unioning the results from all of the
        //AndConditions together.
        Set<AtlasVertex<V, E>> result = new HashSet<>();
        for(AndCondition andExpr : queryCondition_.getAndTerms()) {
            NativeTitanGraphQuery<V,E> andQuery = andExpr.create(getQueryFactory());
            for(AtlasVertex<V,E> vertex : andQuery.vertices()) {
                result.add(vertex);
            }
        }
        return result;
    }

    @Override
    public AtlasGraphQuery<V, E> has(String propertyKey, ComparisionOperator operator,
            Object value) {
        queryCondition_.andWith(new HasPredicate(propertyKey, operator, value));
        return this;
    }


    @Override
    public AtlasGraphQuery<V, E> in(String propertyKey, Collection<? extends Object> values) {
        queryCondition_.andWith(new InPredicate(propertyKey, values));
        return this;
    }

    @Override
    public AtlasGraphQuery<V, E> or(List<AtlasGraphQuery<V, E>> childQueries) {

        //Construct an overall OrCondition by combining all of the children for
        //the OrConditions in all of the childQueries that we passed in.  Then, "and" the current
        //query condition with this overall OrCondition.

        OrCondition overallChildQuery = new OrCondition(false);

        for(AtlasGraphQuery<V, E> atlasChildQuery : childQueries) {
            if(! atlasChildQuery.isChildQuery()) {
                throw new IllegalArgumentException(atlasChildQuery + " is not a child query");
            }
            TitanGraphQuery<V,E> childQuery = (TitanGraphQuery<V,E>)atlasChildQuery;
            overallChildQuery.orWith(childQuery.getOrCondition());
        }

        queryCondition_.andWith(overallChildQuery);
        return this;
    }

    private OrCondition getOrCondition() {
        return queryCondition_;
    }

    @Override
    public AtlasGraphQuery<V, E> addConditionsFrom(AtlasGraphQuery<V, E> otherQuery) {

        TitanGraphQuery<V, E> childQuery = (TitanGraphQuery<V, E>)otherQuery;
        queryCondition_.andWith(childQuery.getOrCondition());
        return this;
    }

    @Override
    public boolean isChildQuery() {
        return isChildQuery_;
    }
}
