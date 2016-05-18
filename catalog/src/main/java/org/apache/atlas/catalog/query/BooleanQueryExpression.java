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

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.filter.AndFilterPipe;
import com.tinkerpop.pipes.filter.OrFilterPipe;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;

import java.util.*;

/**
 * Expression where operands are other expressions and operator is logical AND or OR
 */
public class BooleanQueryExpression extends BaseQueryExpression {
    private final BooleanClause[] clauses;
    private final QueryFactory queryFactory;

    public BooleanQueryExpression(BooleanQuery query, ResourceDefinition resourceDefinition, QueryFactory queryFactory) {
        super(null, null, resourceDefinition);
        clauses = query.getClauses();
        this.queryFactory = queryFactory;
    }

    @Override
    public Pipe asPipe() {
        Map<BooleanClause.Occur, Collection<BooleanClause>> groupedClauses = groupClauses();

        Pipe andPipe = null;
        Collection<Pipe> andPipes = processAndClauses(groupedClauses);
        andPipes.addAll(processNotClauses(groupedClauses));
        if (! andPipes.isEmpty()) {
            andPipe = new AndFilterPipe(andPipes.toArray(new Pipe[andPipes.size()]));
        }

        Collection<Pipe> orPipes = processOrClauses(groupedClauses);
        if (! orPipes.isEmpty()) {
            if (andPipe != null) {
                orPipes.add(andPipe);
            }
            return new OrFilterPipe(orPipes.toArray(new Pipe[orPipes.size()]));
        } else {
            return andPipe;
        }
    }

    private Map<BooleanClause.Occur, Collection<BooleanClause>> groupClauses() {
        Map<BooleanClause.Occur, Collection<BooleanClause>> groupedClauses = new HashMap<>();
        for (BooleanClause clause : clauses) {
            BooleanClause.Occur occur = resolveClauseOccur(clause);
            Collection<BooleanClause> clauseGrouping = groupedClauses.get(occur);
            if (clauseGrouping == null) {
                clauseGrouping = new ArrayList<>();
                groupedClauses.put(occur, clauseGrouping);
            }
            clauseGrouping.add(clause);
        }
        return groupedClauses;
    }

    private BooleanClause.Occur resolveClauseOccur(BooleanClause clause) {
        BooleanClause.Occur occur = clause.getOccur();
        if (negate) {
            switch (occur) {
                case SHOULD:
                    occur = BooleanClause.Occur.MUST_NOT;
                    break;
                case MUST:
                    occur = BooleanClause.Occur.SHOULD;
                    break;
                case MUST_NOT:
                    occur = BooleanClause.Occur.SHOULD;
                    break;
            }
        }
        return occur;
    }

    private Collection<Pipe> processAndClauses(Map<BooleanClause.Occur, Collection<BooleanClause>> groupedClauses) {
        Collection<BooleanClause> andClauses = groupedClauses.get(BooleanClause.Occur.MUST);
        Collection<Pipe> andPipes = new ArrayList<>();
        if (andClauses != null) {
            for (BooleanClause andClause : andClauses) {
                QueryExpression queryExpression = queryFactory.create(andClause.getQuery(), resourceDefinition);
                properties.addAll(queryExpression.getProperties());
                andPipes.add(queryExpression.asPipe());
            }
        }
        return andPipes;
    }


    private Collection<Pipe> processOrClauses(Map<BooleanClause.Occur, Collection<BooleanClause>> groupedClauses) {
        Collection<BooleanClause> shouldClauses = groupedClauses.get(BooleanClause.Occur.SHOULD);
        Collection<Pipe> orPipes = new ArrayList<>();
        if (shouldClauses != null) {
            for (BooleanClause shouldClause : shouldClauses) {
                QueryExpression queryExpression = queryFactory.create(shouldClause.getQuery(), resourceDefinition);
                // don't negate expression if we negated MUST_NOT -> SHOULD
                if (negate && shouldClause.getOccur() != BooleanClause.Occur.MUST_NOT) {
                    queryExpression.setNegate();
                }
                properties.addAll(queryExpression.getProperties());
                orPipes.add(queryExpression.asPipe());
            }
        }
        return orPipes;
    }

    private Collection<Pipe> processNotClauses(Map<BooleanClause.Occur, Collection<BooleanClause>> groupedClauses) {
        Collection<BooleanClause> notClauses = groupedClauses.get(BooleanClause.Occur.MUST_NOT);
        Collection<Pipe> notPipes = new ArrayList<>();
        if (notClauses != null) {
            for (BooleanClause notClause : notClauses) {
                QueryExpression queryExpression = queryFactory.create(notClause.getQuery(), resourceDefinition);
                queryExpression.setNegate();
                properties.addAll(queryExpression.getProperties());
                notPipes.add(queryExpression.asPipe());
            }
        }
        return notPipes;
    }
}
