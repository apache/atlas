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
import org.apache.atlas.groovy.StatementListExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;



/**
 * Optimizer for gremlin queries.  This class provides a framework for applying optimizations
 * to gremlin queries.  Each optimization is implemented as a class that implements {@link GremlinOptimization}.
 *
 * The GremlinQueryOptimizer is the entry point for applying these optimizations.
 *
 *
 */
public final class GremlinQueryOptimizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinQueryOptimizer.class);


    private final List<GremlinOptimization> optimizations = new ArrayList<>();

    //Allows expression factory to be substituted in unit tests.
    private static volatile GremlinExpressionFactory FACTORY = GremlinExpressionFactory.INSTANCE;

    private static volatile GremlinQueryOptimizer INSTANCE = null;

    private GremlinQueryOptimizer() {

    }

    private void addOptimization(GremlinOptimization opt) {
        optimizations.add(opt);
    }

    public static GremlinQueryOptimizer getInstance() {
        if(INSTANCE == null) {
            synchronized(GremlinQueryOptimizer.class) {
                if(INSTANCE == null) {
                    GremlinQueryOptimizer createdInstance = new GremlinQueryOptimizer();
                    //The order here is important.  If there is an "or" nested within an "and",
                    //that will not be found if ExpandOrsOptimization runs before ExpandAndsOptimization.
                    createdInstance.addOptimization(new ExpandAndsOptimization(FACTORY));
                    createdInstance.addOptimization(new ExpandOrsOptimization(FACTORY));
                    INSTANCE = createdInstance;
                }
            }
        }
        return INSTANCE;
    }

    /**
     * For testing only
     */
    @VisibleForTesting
    public static void setExpressionFactory(GremlinExpressionFactory factory) {
        GremlinQueryOptimizer.FACTORY = factory;
    }

    /**
     * For testing only
     */
    @VisibleForTesting
    public static void reset() {
        INSTANCE = null;
    }

    /**
     * Optimizes the provided groovy expression.  Note that the optimization
     * is a <i>destructive</i> process.  The source GroovyExpression will be
     * modified as part of the optimization process.  This is done to avoid
     * expensive copying operations where possible.
     *
     * @param source what to optimize
     * @return the optimized query
     */
    public GroovyExpression optimize(GroovyExpression source) {
        LOGGER.debug("Optimizing gremlin query: " + source);
        OptimizationContext context = new OptimizationContext();
        GroovyExpression updatedExpression = source;
        for (GremlinOptimization opt : optimizations) {
            updatedExpression = optimize(updatedExpression, opt, context);
            LOGGER.debug("After "+ opt.getClass().getSimpleName() + ", query = " + updatedExpression);
        }

        StatementListExpression result = new StatementListExpression();
        result.addStatements(context.getInitialStatements());
        result.addStatement(updatedExpression);
        LOGGER.debug("Final optimized query:  " + result.toString());
        return result;
    }

    /**
     * Optimizes the expression using the given optimization
     * @param source
     * @param optimization
     * @param context
     * @return
     */
    private GroovyExpression optimize(GroovyExpression source, GremlinOptimization optimization,
                                          OptimizationContext context) {
        GroovyExpression result = source;
        if (optimization.appliesTo(source, context)) {
            //Apply the optimization to the expression.
            result = optimization.apply(source, context);
        }
        if (optimization.isApplyRecursively()) {
            //Visit the children, update result with the optimized
            //children.
            List<GroovyExpression> updatedChildren = new ArrayList<>();
            boolean changed = false;
            for (GroovyExpression child : result.getChildren()) {
                //Recursively optimize this child.
                GroovyExpression updatedChild = optimize(child, optimization, context);
                changed |= updatedChild != child;
                updatedChildren.add(updatedChild);
            }
            if (changed) {
                //TBD - Can we update in place rather than making a copy?
                result = result.copy(updatedChildren);
            }
        }
        return result;
    }

    /**
     * Visits all expressions in the call hierarchy of an expression.  For example,
     * in the expression g.V().has('x','y'), the order would be
     * <ol>
     *    <li>pre-visit has('x','y')</li>
     *    <li>pre-visit V()</li>
     *    <li>visit g (non-function caller)</li>
     *    <li>post-visit V()</li>
     *    <li>post-visit has('x','y')</li>
     * </ol>
     * @param expr
     * @param visitor
     */
    public static void visitCallHierarchy(GroovyExpression expr, CallHierarchyVisitor visitor) {

        if (expr == null) {
            visitor.visitNullCaller();
            return;
        }
        if (expr instanceof AbstractFunctionExpression) {
            AbstractFunctionExpression functionCall = (AbstractFunctionExpression)expr;
            if (!visitor.preVisitFunctionCaller(functionCall)) {
                return;
            }
            GroovyExpression caller = functionCall.getCaller();
            visitCallHierarchy(caller, visitor);
            if (!visitor.postVisitFunctionCaller(functionCall)) {
                return;
            }
        } else {
            visitor.visitNonFunctionCaller(expr);
        }
    }

    /**
     * Determines if the given expression is an "or" expression.
     * @param expr
     * @return
     */
    public static boolean isOrExpression(GroovyExpression expr) {
        return IsOr.INSTANCE.apply(expr);
    }

    /**
     * Determines whether the given expression can safely
     * be pulled out of an and/or expression.
     *
     * @param expr an argument to an and or or function
     * @return
     */
    public static boolean isExtractable(GroovyExpression expr) {

        HasForbiddenType hasForbiddenTypePredicate = new HasForbiddenType(FACTORY);

        //alias could conflict with alias in parent traversal
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.SIDE_EFFECT);

        //inlining out(), in() steps will change the result of calls after the and/or()
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.FLAT_MAP_TO_ELEMENTS);
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.FLAT_MAP_TO_VALUES);
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.BARRIER);
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.MAP_TO_ELEMENT);
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.MAP_TO_VALUE);

        //caller expects to be able to continue the traversal.  We can't end it
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.END);


        //we can't inline child traversals
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.SOURCE);
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.START);
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.SIDE_EFFECT);
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.NONE);
        hasForbiddenTypePredicate.addForbiddenType(TraversalStepType.BRANCH);

        ExpressionFinder forbiddenExpressionFinder = new ExpressionFinder(hasForbiddenTypePredicate);
        GremlinQueryOptimizer.visitCallHierarchy(expr, forbiddenExpressionFinder);
        return ! forbiddenExpressionFinder.isExpressionFound();
    }

    /**
     * Recursively copies and follows the caller hierarchy of the expression until we come
     * to a function call with a null caller.  The caller of that expression is set
     * to newLeaf.
     *
     * @param expr
     * @param newLeaf
     * @return the updated (/copied) expression
     */
    public static GroovyExpression copyWithNewLeafNode(AbstractFunctionExpression expr, GroovyExpression newLeaf) {


        AbstractFunctionExpression result = (AbstractFunctionExpression)expr.copy();

        //remove leading anonymous traversal expression, if there is one
        if(FACTORY.isLeafAnonymousTraversalExpression(expr)) {
            result = (AbstractFunctionExpression)newLeaf;
        } else {
            GroovyExpression newCaller = null;
            if (expr.getCaller() == null) {
                newCaller = newLeaf;
            } else {
                newCaller = copyWithNewLeafNode((AbstractFunctionExpression)result.getCaller(), newLeaf);
            }
            result.setCaller(newCaller);
        }
        return result;
    }

}
