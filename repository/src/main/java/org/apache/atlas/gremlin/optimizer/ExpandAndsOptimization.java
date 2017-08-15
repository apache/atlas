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
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizer that pulls has expressions out of an 'and' expression.
 * <p>
 * For example:
 * <pre class=code>
 *   g.V().and(has('x'),has('y') </pre>
 * <p>
 * is optimized to:
 * <pre class=code>
 *   g.V().has('x').has('y') </pre>
 * <p>
 * There are certain cases where it is not safe to move an expression out
 * of the 'and'.  For example, in the expression
 * <pre class=code>
 * g.V().and(has('x').out('y'),has('z')) </pre>
 * <p>
 * has('x').out('y') cannot be moved out of the 'and', since it changes the value of the traverser.
 * <p>
 * At this time, the ExpandAndsOptimizer is not able to handle this scenario, so we don't extract
 * that expression.  In this case, the result is:
 * <pre class=code>
 * g.V().has('z').and(has('x').out('y')) </pre>
 * <p>
 * The optimizer will call ExpandAndsOptimization recursively on the children, so
 * there is no need to recursively update the children here.
 *
 */
public class ExpandAndsOptimization implements GremlinOptimization {

    private static final Logger logger_ = LoggerFactory.getLogger(ExpandAndsOptimization.class);


    private final GremlinExpressionFactory factory;

    public ExpandAndsOptimization(GremlinExpressionFactory factory) {
        this.factory = factory;
    }

    @Override
    public boolean appliesTo(GroovyExpression expr, OptimizationContext contxt) {
        return expr instanceof FunctionCallExpression && ((FunctionCallExpression)expr).getFunctionName().equals("and");
    }

    /**
     * Expands the given and expression.  There is no need to recursively
     * expand the children here.  This method is called recursively by
     * GremlinQueryOptimier on the children.
     *
     */
    @Override
    public GroovyExpression apply(GroovyExpression expr, OptimizationContext context) {

        FunctionCallExpression exprAsFunction = (FunctionCallExpression)expr;
        GroovyExpression result = exprAsFunction.getCaller();

        List<GroovyExpression> nonExtractableArguments = new ArrayList<>();
        for(GroovyExpression argument : exprAsFunction.getArguments()) {

            if (GremlinQueryOptimizer.isExtractable(argument)) {
                //Set the caller of the deepest expression in the call hierarchy
                //of the argument to point to the current result.
                //For example, if result is "g.V()" and the updatedArgument is "has('x').has('y')",
                //updatedArgument would be a tree like this:
                //
                //                  has('y')
                //                 /
                //                / caller
                //              |/_
                //           has('x')
                //             /
                //            / caller
                //          |/_
                //        (null)
                //
                //We would set the caller of has('x') to be g.V(), so result would become g.V().has('x').has('y').
                //
                // Note: This operation is currently done by making a copy of the argument tree.  That should
                // be changed.
                result = GremlinQueryOptimizer.copyWithNewLeafNode(
                        (AbstractFunctionExpression) argument, result);
            } else {
                logger_.warn("Found non-extractable argument '{}' in the 'and' expression '{}'",argument.toString(), expr.toString());
                nonExtractableArguments.add(argument);
            }
        }

        if (!nonExtractableArguments.isEmpty()) {
            //add a final 'and' call with the arguments that could not be extracted
            result =  factory.generateLogicalExpression(result, "and", nonExtractableArguments);
        }
        return result;
    }

    @Override
    public boolean isApplyRecursively() {
        return true;
    }
}
