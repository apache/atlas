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

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.GroovyExpression;


/**
 * Finds all range expressions in the call hierarchy.
 *
 */
public class RangeFinder implements CallHierarchyVisitor {

    private List<AbstractFunctionExpression> rangeExpressions = new ArrayList<>();
    private GremlinExpressionFactory factory;

    public RangeFinder(GremlinExpressionFactory factory) {
        this.factory = factory;
    }

    @Override
    public boolean preVisitFunctionCaller(AbstractFunctionExpression expr) {

        return true;
    }

    @Override
    public void visitNonFunctionCaller(GroovyExpression expr) {
    }

    @Override
    public void visitNullCaller() {
    }

    @Override
    public boolean postVisitFunctionCaller(AbstractFunctionExpression functionCall) {

        if (factory.isRangeExpression(functionCall)) {
            rangeExpressions.add(functionCall);
        }
        return true;
    }

    public List<AbstractFunctionExpression> getRangeExpressions() {
        return rangeExpressions;
    }

}
