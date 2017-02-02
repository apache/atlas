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
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.TraversalStepType;

/**
 * Matches an expression that gets called after calling or().  For example,
 * in g.V().or(x,y).toList(), "toList()" is the "or parent", so calling
 * "apply()" on this expression would return true and calling it on all
 * the other ones would return false.
 */
public final class IsOrParent implements Function<GroovyExpression, Boolean> {

    public static final IsOrParent INSTANCE = new IsOrParent();

    private IsOrParent() {

    }

    @Override
    public Boolean apply(GroovyExpression expr) {
        if (!(expr instanceof AbstractFunctionExpression)) {
            return false;
        }
        AbstractFunctionExpression functionCall = (AbstractFunctionExpression)expr;
        GroovyExpression target = functionCall.getCaller();

        if (!(target instanceof FunctionCallExpression)) {
            return false;
        }

        if (target.getType() != TraversalStepType.FILTER) {
            return false;
        }

        FunctionCallExpression targetFunction = (FunctionCallExpression)target;
        return targetFunction.getFunctionName().equals("or");
    }
}
