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

import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.GroovyExpression;

/**
 * Determines whether an expression contains a repeat/loop function.
 */
public class RepeatExpressionFinder implements CallHierarchyVisitor {

    private boolean found = false;
    private GremlinExpressionFactory factory;

    public RepeatExpressionFinder(GremlinExpressionFactory factory) {
        this.factory = factory;
    }

    @Override
    public boolean preVisitFunctionCaller(AbstractFunctionExpression expr) {

        found = factory.isRepeatExpression(expr);
        if(found) {
            return false;
        }
        return true;
    }

    @Override
    public void visitNonFunctionCaller(GroovyExpression expr) {

    }

    @Override
    public void visitNullCaller() {

    }

    public boolean isRepeatExpressionFound() {
        return found;
    }

    @Override
    public boolean postVisitFunctionCaller(AbstractFunctionExpression functionCall) {

        return false;
    }
}
