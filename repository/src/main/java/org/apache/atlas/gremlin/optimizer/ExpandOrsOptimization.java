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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.ClosureExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.StatementListExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;



/**
 * Optimization that removes 'or' expressions from a graph traversal when possible
 * and replaces them with separate calls that are combined using a logical union operation.
 * Unfortunately, Titan does not use indices when executing the child graph traversals associated
 * with an 'or' call.  In order to make the index be used, we split queries with
 * or expressions into multiple queries.  These queries are executed individually,
 * using indices, and then the results are combined back together.  Here is a
 * simple example to illustrate this:
 *
 * <h4>Original Query</h4>
 *
 * <pre>
 * g.V().or(has('name','Fred'),has('age','17'))
 * </pre>
 *
 *<h4>Optimized Query</h4>
 *
 * <pre>
 * def r = [] as Set;
 * g.V().has('name','Fred').fill(r);
 * g.V().has('age','17').fill(r);
 * r;
 * </pre>
 *
 * Here, we introduce an intermediate variable "r" which is declared as a Set.  The Set is performing
 * the union for us.  If there are vertices that happen to both have "Fred" as the name and "17" as the age,
 * the Set will prevent the second query execution from adding a duplicate vertex to the result.  Recall that
 * in Groovy scripts, the last expression is the one that will be returned back to the caller.  We refer to
 * that expression is the "result expression".  For this example, the result expression is simply "r", which
 * contains the vertices that matched the query.
 * <p/>
 * If the query does any kind of transformation of the vertices to produce the query result, that needs
 * to be done in the result expression.  To understand why that is, let's take a look at another example:
 *
 * <h4>Original Query</h4>
 *
 * <pre>
 * g.V().or(has('name','Fred'),has('age','17')).as('person').select('person').by('gender')
 * </pre>
 *
 * <h4>Incorrect Optimized Query</h4>
 *
 * <pre>
 * def r = [] as Set;
 * g.V().has('name','Fred').as('person').select('person').by('gender').fill(r)
 * g.V().has('age','17').as('person').select('person').by('gender').fill(r)
 * r;
 * </pre>
 *
 * The problem with this query is that now 'r' contains Strings (the gender of the person).  Suppose
 * that there is one person named Fred and there are 3 people whose age is 17 (let's say Fred's age is 16).
 * The original query would have produced 4 rows, one corresponding to each of those people.  The new
 * query would produce at most 2 rows - one for 'male' and one for 'female'.  This is happening because
 * we are now performing the union on the Strings, not on the vertices.  To fix this, we need to split
 * the original query and put the end portion into the result expression:
 *
 * <h4>Correct Optimized Query</h4>
 *
 * <pre>
 * def r = [] as Set;
 * g.V().has('name','Fred').fill(r)
 * g.V().has('age','17').fill(r)
 * __.inject(r as Object[]).as('person').select('person').by('gender')
 * </pre>
 *
 * The logic for doing this splitting is described in more detail in
 * {@link #moveTransformationsToResultExpression(GroovyExpression, OptimizationContext)}.
 * <p/>
 * There is one more problematic case that this optimizer is able to handle.  Let's look at the following example:
 *
 * <h4>Original Query</h4>
 *
 * <pre>
 * g.V().or(has('type','Person'),has('superType','Person')).as('x').has('qualifiedName','Fred').as('y').select('x','y').by('name').by('name')
 * </pre>
 *
 * Queries of this form appear often when translating DSL queries.
 *
 * If we were to optimize this query using the logic described above, we would get something like this:
 *
 * <h4>Incorrect Optimized Query</h4>
 *
 * <pre>
 * def r = [] as Set;
 * g.V().has('type','Person').fill(r);
 * g.V().has('superType','Person').fill(r);
 * __.inject(r as Object[]).as('x').has('qualifiedName','Fred').as('y').select('x','y');
 * </pre>
 *
 * While not strictly incorrect, this query will not perform well since the index on qualifiedName will
 * not be used.  In order for that index to be used, the 'has' expression needs to be part of the original
 * query.  However, if we do that alone, the query will be broken, since the select
 * will now refer to an undefined label:
 *
 * <h4>Incorrect Optimized Query</h4>
 *
 * <pre>
 * def r = [] as Set;
 * g.V().has('type','Person').as('x').has('qualifiedName','Fred').fill(r);
 * g.V().has('superType','Person').as('x').has('qualifiedName','Fred').fill(r);
 * __.inject(r as Object[]).as('y').select('x','y')
 * </pre>
 *
 * To fix this, we need to save the values of the aliased vertices in the original
 * query, and create labels in the result expression that refer to them.  We do this
 * as follows:
 *
 * <h4>Correct Optimized Query</h4>
 *
 * <pre>
 * def r = [] as Set;
 * g.V().has('type','Person').as('x').has('qualifiedName','Fred').as('y').select('x','y').fill(r);
 * g.V().has('superType','Person').as('x').has('qualifiedName','Fred').select('x','y').fill(r);
 * __.inject(r as Object[]).as('__tmp').map({((Map)it.get()).get('x')}).as('x').select('__tmp').map({((Map)it.get()).get('x')}).as('y').select('x','y').by('name').by('name')
 * </pre>
 *
 * This is not pretty, but is the best solution we've found so far for supporting expressions that contain aliases in this optimization.
 * What ends up happening is that r gets populated with alias->Vertex maps.  In the result expression, we make 'x' point
 * to a step where the value in the traverser is the vertex for 'x', and we do the same thing for y.  The <code>select('_tmp')</code> step in the middle restores the value of
 * the traverser back to the map.
 * <p/>
 * The one known issue with the alias rearrangement is that it breaks loop expressions.  As a result, expressions containing loops are currently excluded
 * from this optimization.
 *
 * ExpandOrsOptimization expands the entire expression tree recursively, so it is not invoked
 * recursively by GremlinQueryOptimizer.
 *
 */
public class ExpandOrsOptimization implements GremlinOptimization {

    private static final Logger logger_ = LoggerFactory.getLogger(ExpandOrsOptimization.class);

    private final GremlinExpressionFactory factory;

    public ExpandOrsOptimization(GremlinExpressionFactory factory) {
        this.factory = factory;
    }

    @Override
    public boolean appliesTo(GroovyExpression expr, OptimizationContext contxt) {

        ExpressionFinder finder = new ExpressionFinder(IsOr.INSTANCE);
        GremlinQueryOptimizer.visitCallHierarchy(expr, finder);
        return finder.isExpressionFound();
    }

    @Override
    public GroovyExpression apply(GroovyExpression expr, OptimizationContext context) {

        setupRangeOptimization(expr, context);
        GroovyExpression traveralExpression = moveTransformationsToResultExpression(expr, context);

        FunctionGenerator functionGenerator = new FunctionGenerator(factory, context);
        GremlinQueryOptimizer.visitCallHierarchy(traveralExpression, functionGenerator);
        traveralExpression = functionGenerator.getNewRootExpression();
        List<GroovyExpression> bodyExpressions = expandOrs(traveralExpression, context);


        //Adds a statement to define the result variable 'v' in the
        //groovy script.  The variable is declared as a Set.  The type
        //of the objects in the Set depend on the number of aliases in the Groovy
        // expression:
        //   - 0 or 1 alias : Vertex
        //   - multiple aliases: Map<String,Vertex>
        StatementListExpression result = new StatementListExpression();
        context.prependStatement(context.getDefineResultVariableStmt());


        for (GroovyExpression bodyExpression : bodyExpressions) {
            result.addStatement(bodyExpression);
        }
        result.addStatement(context.getResultExpression());
        return result;
    }

    private void setupRangeOptimization(GroovyExpression expr, OptimizationContext context) {

        // Find any range expressions in the expression tree.
        RangeFinder rangeFinder = new RangeFinder(factory);
        GremlinQueryOptimizer.visitCallHierarchy(expr, rangeFinder);
        List<AbstractFunctionExpression> rangeExpressions = rangeFinder.getRangeExpressions();
        if (rangeExpressions.size() == 1) {
            OrderFinder orderFinder = new OrderFinder(factory);
            GremlinQueryOptimizer.visitCallHierarchy(expr, orderFinder);
            if (!orderFinder.hasOrderExpression()) {
                // If there is one range expression and no order expression in the unoptimized gremlin,
                // save the range parameters to use for adding a range expression to
                // each expanded "or" expression result, such that it will only contain the specified range of vertices.
                // For now, apply this optimization only if the range start index is zero.
                AbstractFunctionExpression rangeExpression = rangeExpressions.get(0);
                int[] rangeParameters = factory.getRangeParameters(rangeExpression);
                if (rangeParameters[0] == 0) {
                    context.setRangeExpression(rangeExpression);
                }
            }
        }
    }

    private GroovyExpression moveTransformationsToResultExpression(GroovyExpression expr, OptimizationContext context) {
        GroovyExpression traveralExpression = expr;

        // Determine the 'split point'.  This is the expression that will become
        // the deepest function call in the result expression.  If a split
        // point is found, its caller is changed.  The new caller is
        // set to the graph traversal expression in the result expression.
        // The original caller becomes the new traversal expression that
        // will be carried through the rest of the 'or' expansion processing.
        //
        // Example: g.V().has('x').as('x').select('x')
        // Here, select('x') is the split expression
        // so :
        // 1) the result expression in OptimizationContext becomes [base result expression].select('x')
        // 2) we return g.V().has('x').as('x')

        SplitPointFinder finder = new SplitPointFinder(factory);
        GremlinQueryOptimizer.visitCallHierarchy(traveralExpression, finder);
        AbstractFunctionExpression splitPoint = finder.getSplitPoint();


        List<LiteralExpression> aliases = new ArrayList<>();

        //If we're not splitting the query, there is no need to save/restore
        //the aliases.
        if(splitPoint != null) {

            traveralExpression = splitPoint.getCaller();

            AliasFinder aliasFinder = new AliasFinder();
            GremlinQueryOptimizer.visitCallHierarchy(traveralExpression, aliasFinder);
            aliases.addAll(aliasFinder.getAliases());
            if(aliasFinder.isFinalAliasNeeded()) {
                //The last alias in the expression does not capture the final vertex in the traverser,
                //so we need to create an alias to record that.
                traveralExpression = factory.generateAliasExpression(traveralExpression, context.getFinalAliasName());
                aliases.add(new LiteralExpression(context.getFinalAliasName()));
            }

            GroovyExpression resultExpr = getBaseResultExpression(context, aliases);
            splitPoint.setCaller(resultExpr);
            expr = removeMapFromPathsIfNeeded(expr, aliases);
            context.setResultExpression(expr);
        }

        //Add expression(s) to the end of the traversal expression to add the vertices
        //that were found into the intermediate variable ('r')
        traveralExpression = addCallToUpdateResultVariable(traveralExpression, aliases, context);
        return traveralExpression;
    }

    private GroovyExpression removeMapFromPathsIfNeeded(GroovyExpression expr, List<LiteralExpression> aliases) {
        if(aliases.size() > 0 && factory.isSelectGeneratesMap(aliases.size())) {
            RepeatExpressionFinder repeatExprFinder = new RepeatExpressionFinder(factory);
            GremlinQueryOptimizer.visitCallHierarchy(expr, repeatExprFinder);
            boolean hasRepeat = repeatExprFinder.isRepeatExpressionFound();

            PathExpressionFinder pathExprFinder = new PathExpressionFinder();
            GremlinQueryOptimizer.visitCallHierarchy(expr, pathExprFinder);
            boolean hasPath = pathExprFinder.isPathExpressionFound();
            if(! hasRepeat && hasPath) {
                //the path will now start with the map that we added.  That is an artifact
                //of the optimization process and must be removed.
                if(expr.getType() != TraversalStepType.END && expr.getType() != TraversalStepType.NONE) {
                    //we're still in the pipeline, need to execute the query before we can
                    //modify the result
                    expr = factory.generateToListExpression(expr);
                }
                expr = factory.removeExtraMapFromPathInResult(expr);
            }

        }
        return expr;
    }

    /**
     * This method adds steps to the end of the initial traversal to add the vertices
     * that were found into an intermediate variable (defined as a Set).  If there is one alias,
     * this set will contain the vertices associated with that Alias.  If there are multiple
     * aliases, the values in the set will be alias->vertex maps that have the vertex
     * associated with the alias for each result.

     * @param expr
     * @param aliasNames
     * @param context
     * @return
     */
    private GroovyExpression addCallToUpdateResultVariable(GroovyExpression expr,List<LiteralExpression> aliasNames, OptimizationContext context) {

        GroovyExpression result = expr;
        // If there is one range expression in the unoptimized gremlin,
        // add a range expression here so that the intermediate variable will only contain
        // the specified range of vertices.
        AbstractFunctionExpression rangeExpression = context.getRangeExpression();
        if (rangeExpression != null) {
            int[] rangeParameters = factory.getRangeParameters(rangeExpression);
            result = factory.generateRangeExpression(result, rangeParameters[0], rangeParameters[1]);
        }
        if( ! aliasNames.isEmpty()) {
            result = factory.generateSelectExpression(result, aliasNames, Collections.<GroovyExpression>emptyList());
        }
        return factory.generateFillExpression(result, context.getResultVariable());
    }

    /**
     * Recursively traverses the given expression, expanding or expressions
     * wherever they are found.
     *
     * @param expr
     * @param context
     * @return expressions that should be unioned together to get the query result
     */
    private List<GroovyExpression> expandOrs(GroovyExpression expr, OptimizationContext context) {

        if (GremlinQueryOptimizer.isOrExpression(expr)) {
            return expandOrFunction(expr, context);
        }
        return processOtherExpression(expr, context);
    }

    /**
     * This method takes an 'or' expression and expands it into multiple expressions.
     *
     * For example:
     *
     * g.V().or(has('x'),has('y')
     *
     * is expanded to:
     *
     * g.V().has('x')
     * g.V().has('y')
     *
     * There are certain cases where it is not safe to move an expression out
     * of the 'or'.  For example, in the expression
     *
     * g.V().or(has('x').out('y'),has('z'))
     *
     * has('x').out('y') cannot be moved out of the 'or', since it changes the value of the traverser.
     *
     * At this time, the ExpandOrsOptimizer is not able to handle this scenario, so we don't remove
     * that expression.  In cases like this, a final expression is created that ors together
     * all of the expressions that could not be extracted.  In this case that would be:
     *
     * g.V().has('z')
     * g.V().or(has('y').out('z'))
     *
     * This processing is done recursively.
     *
     *
     * @param expr
     * @param context
     * @return the expressions that should be unioned together to get the query result
     */
    private List<GroovyExpression> expandOrFunction(GroovyExpression expr, OptimizationContext context) {
        FunctionCallExpression functionCall = (FunctionCallExpression) expr;
        GroovyExpression caller = functionCall.getCaller();
        List<GroovyExpression> updatedCallers = null;
        if (caller != null) {
            updatedCallers = expandOrs(caller, context);
        } else {
            updatedCallers = Collections.singletonList(null);
        }
        UpdatedExpressions newArguments = getUpdatedChildren(functionCall.getArguments(), context);
        List<GroovyExpression> allUpdatedArguments = new ArrayList<>();
        for (List<GroovyExpression> exprs : newArguments.getUpdatedChildren()) {
            allUpdatedArguments.addAll(exprs);
        }
        List<AbstractFunctionExpression> extractableArguments = new ArrayList<>();
        List<GroovyExpression> nonExtractableArguments = new ArrayList<>();
        for (GroovyExpression argument : allUpdatedArguments) {

            if (GremlinQueryOptimizer.isExtractable(argument)) {
                extractableArguments.add((AbstractFunctionExpression) argument);
            } else {
                logger_.warn("Found non-extractable argument '{}; in the 'or' expression '{}'",argument.toString(), expr.toString());
                nonExtractableArguments.add(argument);
            }
        }

        List<GroovyExpression> result = new ArrayList<>();
        for (GroovyExpression updatedCaller : updatedCallers) {

            for (AbstractFunctionExpression arg : extractableArguments) {
                GroovyExpression updated = GremlinQueryOptimizer.copyWithNewLeafNode(arg, updatedCaller);
                result.add(updated);
            }
            if (!nonExtractableArguments.isEmpty()) {
                result.add(factory.generateLogicalExpression(updatedCaller, "or", nonExtractableArguments));
            }

        }
        return result;
    }

    private UpdatedExpressions getUpdatedChildren(List<GroovyExpression> children, OptimizationContext context) {
        List<List<GroovyExpression>> updatedChildren = new ArrayList<>();
        boolean changed = false;
        for (GroovyExpression child : children) {
            List<GroovyExpression> childChoices = expandOrs(child, context);
            if (childChoices.size() != 1 || childChoices.iterator().next() != child) {
                changed = true;
            }
            updatedChildren.add(childChoices);
        }
        return new UpdatedExpressions(changed, updatedChildren);
    }

    private UpdatedExpressions getUpdatedChildren(GroovyExpression expr, OptimizationContext context) {
        return getUpdatedChildren(expr.getChildren(), context);
    }

    /**
     * This is called when we encounter an expression that is not an "or", for example an "and" expressio.  For these
     * expressions, we process the children and create copies with the cartesian product of the updated
     * arguments.
     *
     * Example:
     *
     * g.V().and(or(has('x),has('y'), or(has('a'),has('b')))
     *
     * Here, we have an "and" expression with two children:
     *
     * 1) or(has('x),has('y')
     * 2) or(has('a'),has('b'))
     *
     * We first process these children.  They each yield 2 expressions:
     *
     * 1 -> [ has('x'), has('y') ]
     * 2 -> [ has('a'), has('b') ]
     *
     * The cartesian product of these gives this:
     *
     *    [ has('x'), has('a') ]
     *    [ has('x'), has('b') ]
     *    [ has('y'), has('a') ]
     *    [ has('y'), has('b') ]
     *
     * So the overall result is:
     *
     * g.V().and(has('x'), has('a'))
     * g.V().and(has('x'), has('b'))
     * g.V().and(has('y'), has('a'))
     * g.V().and(has('y'), has('b'))
     *
     *
     * @param source
     * @param context
     * @return expressions that should be unioned together to get the query result
     */
    private List<GroovyExpression> processOtherExpression(GroovyExpression source, OptimizationContext context) {
        UpdatedExpressions updatedChildren = getUpdatedChildren(source, context);
        if (!updatedChildren.hasChanges()) {
            return Collections.singletonList(source);
        }
        List<GroovyExpression> result = new ArrayList<GroovyExpression>();

        //The updated children list we get back has the possible values for each child
        //in the expression.  We compute a cartesian product to get all possible
        //combinations of child values.
        List<List<GroovyExpression>> updateChildLists = Lists.cartesianProduct(updatedChildren.getUpdatedChildren());

        for (List<GroovyExpression> updatedChildList : updateChildLists) {
            result.add(source.copy(updatedChildList));
        }
        return result;
    }

    @Override
    public boolean isApplyRecursively() {
        return false;
    }

    /**
     *
     * This method creates a base result expression that recreates the state of the
     * graph traverser at start of the result expression to what it would have been
     * if we had been executing one Gremlin query (instead of many and doing a union).
     *
     * To do this, we start with an anonymous graph traversal that will iterate
     * through the values in the intermediate Set that was created.  We then need
     * to set things up so that the aliases that were in the original gremlin query
     * refer to steps with the correct traverser value.
     *
     * The way we do this depends on the number of aliases.  If there are 0 or 1 alias,
     * the intermediate variable already contains Vertices, so we just create the alias.
     *
     * If there are multiple aliases, the intermediate variable contains a String->Vertex
     * map.  We first create a temporary alias that refers to that map.  For each alias,
     * we use a MapStep to map the map to the Vertex for that alias.  We then add back
     * the alias, making it refer to the MapStep.  Between the alias restorations, we restore the
     * traverser object back to the map.
     *
     * @param context
     * @param aliases
     * @return
     */
    private GroovyExpression getBaseResultExpression(OptimizationContext context,
                                                            List<LiteralExpression> aliases) {

        //Start with an anonymous traversal that gets its objects from the intermediate result variable.
        GroovyExpression parent = factory.generateSeededTraversalExpresssion(aliases.size() > 1, context.getResultVariable());

        if(aliases.isEmpty()) {
            return parent;
        }

        //The expression we will return.
        GroovyExpression result = parent;

        //We use a temporary alias to save/restore the original value of the traverser
        //at the start of the query.  We do this so we can set the value of the traverser
        //back to being the map after we retrieve each alias.  If there is only one
        //alias, the save/restore is not needed, so there is no need to create this alias.
        if(aliases.size() > 1) {

            result = factory.generateAliasExpression(result, context.getTempAliasName());
        }

        Iterator<LiteralExpression> it = aliases.iterator();
        while(it.hasNext()) {
            LiteralExpression curAlias = it.next();
            //A map is only generated by Gremlin when there is more than one alias.  When there is only one
            //alias, the intermediate variable will directly contain the vertices.`
            if(factory.isSelectGeneratesMap(aliases.size())) {
                //Since there is more than one alias, the current traverser object is an alias->vertex
                //map.  We use a MapStep to map that map to the Vertex for the current alias.  This sets
                //the current traverser object to that Vertex.  We do this by defining the closure we
                //pass to the MapStep call [map].get(aliasName) where [map] is the expression
                //that refers to the map.

                GroovyExpression rowMapExpr = factory.getCurrentTraverserObject(factory.getClosureArgumentValue());
                GroovyExpression getExpr = factory.generateGetSelectedValueExpression(curAlias, rowMapExpr);
                result = factory.generateMapExpression(result, new ClosureExpression(getExpr));
            }

            //Create alias that points to the previous step.  The traverser value at that step
            //is the Vertex associated with this alias.
            result = factory.generateAliasExpression(result, curAlias.getValue().toString());
            if(it.hasNext()) {
                //Restore the current value of the traverser back to the current alias->vertex map
                result = factory.generateBackReferenceExpression(result, false, context.getTempAliasName());
            }
        }
        return result;
    }




}

