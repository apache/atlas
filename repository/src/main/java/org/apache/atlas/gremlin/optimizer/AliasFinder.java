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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.TraversalStepType;

/**
 * Finds all aliases in the expression.
 */
public class AliasFinder implements CallHierarchyVisitor {

    private List<LiteralExpression> foundAliases = new ArrayList<>();

    //Whether a final alias is needed.  A final alias is needed
    //if there are transformation steps after the last alias in
    //the expression.  We initialize this to false since a final
    //alias is not needed if there are no aliases.
    private boolean finalAliasNeeded = false;

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

    private static final Set<TraversalStepType> TRANSFORMATION_STEP_TYPES = new HashSet<>(Arrays.asList(
            TraversalStepType.MAP_TO_ELEMENT,
            TraversalStepType.MAP_TO_VALUE,
            TraversalStepType.FLAT_MAP_TO_ELEMENTS,
            TraversalStepType.FLAT_MAP_TO_VALUES,
            TraversalStepType.BARRIER,
            TraversalStepType.NONE));


    @Override
    public boolean postVisitFunctionCaller(AbstractFunctionExpression functionCall) {

        if (functionCall instanceof FunctionCallExpression) {
            FunctionCallExpression expr = (FunctionCallExpression)functionCall;
            if (expr.getType() == TraversalStepType.SIDE_EFFECT && expr.getFunctionName().equals("as")) {
                //We found an alias.  This is currently the last expression we've seen
                //in our traversal back up the expression tree, so at this point a final
                //alias is not needed.
                LiteralExpression aliasNameExpr = (LiteralExpression)expr.getArguments().get(0);
                foundAliases.add(aliasNameExpr);
                finalAliasNeeded=false;
            }
        }

        if(TRANSFORMATION_STEP_TYPES.contains(functionCall.getType())) {
            //This step changes the value of the traverser.  Now, a final alias
            //needs to be added.
            if(!foundAliases.isEmpty()) {
                finalAliasNeeded = true;
            }
        }

        return true;
    }

    public List<LiteralExpression> getAliases() {
        return foundAliases;
    }

    public boolean isFinalAliasNeeded() {

        return finalAliasNeeded;
    }
}
