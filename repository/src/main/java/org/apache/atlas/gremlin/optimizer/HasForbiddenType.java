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
package org.apache.atlas.gremlin.optimizer;

import java.util.HashSet;
import java.util.Set;
import com.google.common.base.Function;

import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.TraversalStepType;

/**
 * Function that tests whether the expression is an 'or'
 * graph traversal function.
 */
public final class HasForbiddenType implements Function<GroovyExpression, Boolean> {

    private Set<TraversalStepType> forbiddenTypes =  new HashSet<>();
    private final GremlinExpressionFactory factory;

    public HasForbiddenType(GremlinExpressionFactory factory) {
        this.factory = factory;
    }

    public void addForbiddenType(TraversalStepType type) {
        forbiddenTypes.add(type);
    }

    @Override
    public Boolean apply(GroovyExpression expr) {
        if(factory.isLeafAnonymousTraversalExpression(expr)) {
            return false;
        }
        return forbiddenTypes.contains(expr.getType());
    }
}
