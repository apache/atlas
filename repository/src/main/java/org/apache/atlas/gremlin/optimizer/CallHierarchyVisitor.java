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

import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.GroovyExpression;

/**
 * Call back interface for visiting the call hierarchy of a function call.
 */
public interface CallHierarchyVisitor {

    /**
     * Visits a function expression before the visit to its caller.
     *
     * @param expr
     *
     * @return false to terminate the recursion
     */
    boolean preVisitFunctionCaller(AbstractFunctionExpression expr);

    /**
     * Called when a caller that is not an instance of
     * AbstractFunctionExpression is found. This indicates that the deepest
     * point in the call hierarchy has been reached.
     *
     *
     */
    void visitNonFunctionCaller(GroovyExpression expr);

    /**
     * Called when a null caller is found (this happens for static/user-defined
     * functions). This indicates that the deepest point in the call hierarchy
     * has been reached.
     *
     */
    void visitNullCaller();

    /**
     * Visits a function expression after the visit to its caller.
     *
     * @param expr
     *
     * @return false to terminate the recursion
     */
    boolean postVisitFunctionCaller(AbstractFunctionExpression functionCall);
}
