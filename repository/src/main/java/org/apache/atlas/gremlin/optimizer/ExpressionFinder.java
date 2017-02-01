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

import com.google.common.base.Function;

import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.GroovyExpression;

/**
 * Call hierarchy visitor that checks if an expression
 * matching the specified criteria is present
 * in the call hierarch.
 */
public class ExpressionFinder implements CallHierarchyVisitor {

    private final Function<GroovyExpression, Boolean> predicate;
    private boolean expressionFound = false;

    public ExpressionFinder(Function<GroovyExpression, Boolean> predicate) {
        this.predicate = predicate;
    }
    @Override
    public boolean preVisitFunctionCaller(AbstractFunctionExpression expr) {
        if (predicate.apply(expr)) {
            expressionFound = true;
            return false;
        }
        return true;
    }

    @Override
    public void visitNonFunctionCaller(GroovyExpression expr) {
        if (predicate.apply(expr)) {
            expressionFound = true;
        }
    }

    @Override
    public void visitNullCaller() {
        //nothing to do
    }

    @Override
    public boolean postVisitFunctionCaller(AbstractFunctionExpression functionCall) {
        //nothing to do
        return true;
    }

    public boolean isExpressionFound() {
        return expressionFound;
    }

}
