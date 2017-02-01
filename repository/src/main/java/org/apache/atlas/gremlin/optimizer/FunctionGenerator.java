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

import java.util.List;

import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.ClosureExpression;
import org.apache.atlas.groovy.ClosureExpression.VariableDeclaration;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;

/**
 * Extracts common expressions from an or-containing expression
 * into functions.  These expressions would otherwise be duplicated
 * as part of expanding the "or".  Doing this shortens the overall length
 * of the Gremlin script so we can maximize query performance.
 *
 */
public class FunctionGenerator implements CallHierarchyVisitor {

    //Function length constants.
    //These assume we won't reach more than 9 function definition.  Even if we do, this is still
    //a reasonable approximation.
    private static final int INITIAL_FUNCTION_DEF_LENGTH = "def f1={};".length();
    private final int functionDefLength;
    private static final int FUNCTION_CALL_OVERHEAD = "f1()".length();

    /**
     * The expression that should be the first (deepest) expression
     * in the body of the next generated function.  As we go up the
     * expression tree in the post visit, this is updated based on the
     * expressions we see.  During the post visits, if it is null,
     * the body expression is set to the expression we're visiting.
     * As we go up the tree, it is nulled out if we create a function
     * or encounter an or expression.  This guarantees that the
     * next function body will not contain any or expressions
     * and that it will not have expressions that are already
     * part of some other function.
     */
    private GroovyExpression nextFunctionBodyStart;

    /**
     * The number of times expressions will be duplicated.
     */
    private int scaleFactor = 1;

    private final OptimizationContext context;

    /**
     * The current depth in the expression tree.
     */
    private int depth = 0;

    /**
     * The name of the last function that was generated.  If set,
     * we can safely update this function instead of creating a new one.
     */
    private String currentFunctionName;

    /**
     * The updated expression we will pass back to the caller.
     */
    private GroovyExpression newRootExpression;

    private final GremlinExpressionFactory factory;

    public FunctionGenerator(GremlinExpressionFactory factory, OptimizationContext context) {
        this.context = context;
        this.factory = factory;
        functionDefLength = ("def f1={" + factory.getTraversalExpressionClass() + " x->};").length();
    }

    @Override
    public boolean preVisitFunctionCaller(AbstractFunctionExpression expr) {
        depth++;
        if (IsOr.INSTANCE.apply(expr)) {
            FunctionCallExpression functionCall = (FunctionCallExpression) expr;
            scaleFactor *= functionCall.getArguments().size();
        }
        if (newRootExpression == null) {
            newRootExpression = expr;
        }

        return true;
    }

    @Override
    public void visitNonFunctionCaller(GroovyExpression expr) {
        if (nextFunctionBodyStart == null) {
            nextFunctionBodyStart = expr;
        }

    }

    @Override
    public void visitNullCaller() {
        //nothing to do
    }

    @Override
    public boolean postVisitFunctionCaller(AbstractFunctionExpression expr) {
        boolean isRootExpr = depth == 1;
        visitParentExpression(expr);

        //The root expression has no parent.  To simplify the logic, we create
        //a dummy expression so it does have a parent, then call visitParentExpression again
        //to examine the root expression.
        if (isRootExpr) {
            FunctionCallExpression dummyParent = new FunctionCallExpression(expr, "dummy");
            visitParentExpression(dummyParent);
            newRootExpression = dummyParent.getCaller();
        }

        depth--;
        return true;
    }

    /**
     * Checks to see if the *caller* of this expression should become part
     * of a function.  If so, either a new function is created, or the
     * expression becomes part of the last function we created.
     *
     * @param parentExpr
     */
    private void visitParentExpression(AbstractFunctionExpression parentExpr) {

        if (nextFunctionBodyStart == null) {
            nextFunctionBodyStart = parentExpr;
        }

        if (currentFunctionName != null) {
            updateCurrentFunction(parentExpr);
        } else {
            createFunctionIfNeeded(parentExpr);
        }

        if (GremlinQueryOptimizer.isOrExpression(parentExpr)) {
            //reset
            currentFunctionName = null;
            //don't include 'or' in generated functions
            nextFunctionBodyStart = null;
        }

    }

    /**
     * Creates a function whose body goes from the child of parentExpr
     * up to (and including) the functionBodyEndExpr.
     * @param parentExpr
     */
    private void createFunctionIfNeeded(AbstractFunctionExpression parentExpr) {
        GroovyExpression potentialFunctionBody = parentExpr.getCaller();

        if (creatingFunctionShortensGremlin(potentialFunctionBody)) {
            GroovyExpression functionCall = null;

            if (nextFunctionBodyStart instanceof AbstractFunctionExpression) {
                //The function body start is a a function call.  In this
                //case, we generate a function that takes one argument, which
                //is a graph traversal.  We have an expression tree that
                //looks kind of like the following:
                //
                //                     parentExpr
                //                       /
                //                      / caller
                //                    |/_
                //           potentialFunctionBody
                //                   /
                //                  / caller
                //                |/_
                //               ...
                //               /
                //              / caller
                //            |/_
                //    nextFunctionBodyStart
                //           /
                //          / caller
                //        |/_
                //    oldCaller
                //
                //
                // Note that potentialFunctionBody and nextFunctionBodyStart
                // could be the same expression.  Let's say that the next
                // function name is f1
                //
                // We reshuffle these expressions to the following:
                //
                //                     parentExpr
                //                       /
                //                      / caller
                //                    |/_
                //                f1(oldCaller)
                //
                //
                //           potentialFunctionBody   <- body of new function "f1(GraphTraversal x)"
                //                   /
                //                  / caller
                //                |/_
                //               ...
                //               /
                //              / caller
                //            |/_
                //    nextFunctionBodyStart
                //           /
                //          / caller
                //        |/_
                //        x
                //
                // As an example, suppose parentExpr is g.V().or(x,y).has(a).has(b).has(c)
                // where has(a) is nextFunctionBodyStart.
                //
                // We generate a function f1 = { GraphTraversal x -> x.has(a).has(b) }
                // parentExpr would become : f1(g.V().or(x,y)).has(c)

                AbstractFunctionExpression nextFunctionBodyStartFunction=
                        (AbstractFunctionExpression) nextFunctionBodyStart;
                String variableName = "x";
                IdentifierExpression var = new IdentifierExpression(variableName);
                GroovyExpression oldCaller = nextFunctionBodyStartFunction.getCaller();
                nextFunctionBodyStartFunction.setCaller(var);

                currentFunctionName = context.addFunctionDefinition(new VariableDeclaration(factory.getTraversalExpressionClass(), "x"),
                        potentialFunctionBody);
                functionCall = new FunctionCallExpression(potentialFunctionBody.getType(),
                        currentFunctionName, oldCaller);

            } else {
                //The function body start is a not a function call.  In this
                //case, we generate a function that takes no arguments.

                // As an example, suppose parentExpr is g.V().has(a).has(b).has(c)
                // where g is nextFunctionBodyStart.
                //
                // We generate a function f1 = { g.V().has(a).has(b) }
                // parentExpr would become : f1().has(c)

                currentFunctionName = context.addFunctionDefinition(null, potentialFunctionBody);
                functionCall = new FunctionCallExpression(potentialFunctionBody.getType(), currentFunctionName);
            }

            //functionBodyEnd is now part of a function definition, don't propagate it
            nextFunctionBodyStart = null;
            parentExpr.setCaller(functionCall);
        }
    }

    /**
     * Adds the caller of parentExpr to the current body of the last
     * function that was created.
     *
     * @param parentExpr
     */
    private void updateCurrentFunction(AbstractFunctionExpression parentExpr) {
        GroovyExpression expr = parentExpr.getCaller();
        if (expr instanceof AbstractFunctionExpression) {
            AbstractFunctionExpression exprAsFunction = (AbstractFunctionExpression) expr;
            GroovyExpression exprCaller = exprAsFunction.getCaller();
            parentExpr.setCaller(exprCaller);
            updateCurrentFunctionDefintion(exprAsFunction);
        }
    }

    private void updateCurrentFunctionDefintion(AbstractFunctionExpression exprToAdd) {
        ClosureExpression functionBodyClosure = context.getUserDefinedFunctionBody(currentFunctionName);
        if (functionBodyClosure == null) {
            throw new IllegalStateException("User-defined function " + currentFunctionName + " not found!");
        }
        List<GroovyExpression> exprs = functionBodyClosure.getStatements();
        GroovyExpression currentFunctionBody = exprs.get(exprs.size() - 1);
        //Update the expression so it is called by the current return
        //value of the function.
        exprToAdd.setCaller(currentFunctionBody);
        functionBodyClosure.replaceStatement(exprs.size() - 1, exprToAdd);
    }

    //Determines if extracting this expression into a function will shorten
    //the overall length of the Groovy script.
    private boolean creatingFunctionShortensGremlin(GroovyExpression headExpr) {
        int tailLength = getTailLength();
        int length = headExpr.toString().length() - tailLength;

        int overhead = 0;
        if (nextFunctionBodyStart instanceof AbstractFunctionExpression) {
            overhead = functionDefLength;
        } else {
            overhead = INITIAL_FUNCTION_DEF_LENGTH;
        }
        overhead += FUNCTION_CALL_OVERHEAD * scaleFactor;
        //length * scaleFactor = space taken by having the expression be inlined [scaleFactor] times
        //overhead + length = space taken by the function definition and its calls
        return length * scaleFactor > overhead + length;
    }

    private int getTailLength() {
        if (nextFunctionBodyStart == null) {
            return 0;
        }
        if (!(nextFunctionBodyStart instanceof AbstractFunctionExpression)) {
            return 0;
        }
        AbstractFunctionExpression bodyEndAsFunction = (AbstractFunctionExpression) nextFunctionBodyStart;
        return bodyEndAsFunction.getCaller().toString().length();
    }

    public GroovyExpression getNewRootExpression() {
        return newRootExpression;
    }
}
