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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.TraversalStepType;


/**
 * This class finds the first place in the expression where the value of the
 * traverser is changed from being a vertex to being something else.  This is
 * important in the "or" optimization logic, since the union operation must be
 * done on *vertices* in order to preserve the semantics of the query.  In addition,
 * expressions that have side effects must be moved as well, so that those
 * side effects will be available to the steps that need them.
 */
public class SplitPointFinder implements CallHierarchyVisitor {

    //Any steps that change the traverser value to something that is not a vertex or edge
    //must be included here, so that the union created by ExpandOrsOptimization
    //is done over vertices/edges.
    private static final Set<TraversalStepType> TYPES_REQUIRED_IN_RESULT_EXPRESSION = new HashSet<>(
            Arrays.asList(
                    TraversalStepType.BARRIER,
                    TraversalStepType.BRANCH,
                    TraversalStepType.SIDE_EFFECT,
                    TraversalStepType.MAP_TO_VALUE,
                    TraversalStepType.FLAT_MAP_TO_VALUES,
                    TraversalStepType.END,
                    TraversalStepType.NONE));

    private final Set<String> requiredAliases = new HashSet<>();

    //Exceptions to the requirement that all expressions with a type
    //in the above list must be in the result expression.  If the
    //function name is in this list, it is ok for that expression
    //to not be in the result expression.  This mechanism allows
    //aliases to remain outside the result expression.  Other
    //exceptions may be found in the future.
    private static final Map<TraversalStepType, WhiteList> WHITE_LISTS = new HashMap<>();
    static {
        WHITE_LISTS.put(TraversalStepType.SIDE_EFFECT, new WhiteList("as"));
    }

    private final GremlinExpressionFactory factory;

    public SplitPointFinder(GremlinExpressionFactory factory) {
        this.factory = factory;
    }

    /**
     * Represents a set of function names.
     */
    private static final class WhiteList {
        private Set<String> allowedFunctionNames = new HashSet<>();
        public WhiteList(String... names) {
            for(String name : names) {
                allowedFunctionNames.add(name);
            }
        }
        public boolean contains(String name) {
            return allowedFunctionNames.contains(name);
        }
    }

    private AbstractFunctionExpression splitPoint;

    @Override
    public boolean preVisitFunctionCaller(AbstractFunctionExpression expr) {
        requiredAliases.addAll(factory.getAliasesRequiredByExpression(expr));
        return true;
    }

    @Override
    public void visitNonFunctionCaller(GroovyExpression expr) {

    }

    @Override
    public void visitNullCaller() {

    }

    public AbstractFunctionExpression getSplitPoint() {
        return splitPoint;
    }

    @Override
    public boolean postVisitFunctionCaller(AbstractFunctionExpression functionCall) {
        String aliasName = factory.getAliasNameIfRelevant(functionCall);
        if (splitPoint == null) {

            boolean required = isRequiredAlias(aliasName) ||
                    isRequiredInResultExpression(functionCall);
            if (required) {
                splitPoint = functionCall;
            }
        }
        removeSeenAlias(aliasName);

        return true;
    }

    private void removeSeenAlias(String aliasName) {
        if(aliasName != null) {
            requiredAliases.remove(aliasName);
        }
    }

    private boolean isRequiredAlias(String aliasName) {
        if(aliasName != null) {
            return requiredAliases.contains(aliasName);
        }
        return false;
    }

    private boolean isRequiredInResultExpression(AbstractFunctionExpression expr) {

        TraversalStepType type = expr.getType();
        if (!TYPES_REQUIRED_IN_RESULT_EXPRESSION.contains(type)) {
            return false;
        }

        if(expr instanceof FunctionCallExpression) {
            FunctionCallExpression functionCall = (FunctionCallExpression)expr;
            //check if the white list permits this function call.  If there is
            //no white list, all expressions with the current step type must go in the
            //result expression.
            WhiteList whiteList = WHITE_LISTS.get(type);
            if(whiteList != null && whiteList.contains(functionCall.getFunctionName())) {
                return false;
            }
        }
        return true;

    }
}
