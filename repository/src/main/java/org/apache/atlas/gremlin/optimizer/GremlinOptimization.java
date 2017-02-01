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

import org.apache.atlas.groovy.GroovyExpression;

/**
 * An optimization that can be applied to a gremlin query.
 */
public interface GremlinOptimization {

    /**
     * Whether or not this optimization should be applied to the given expression
     * @param expr
     * @param contxt
     * @return
     */
    boolean appliesTo(GroovyExpression expr, OptimizationContext contxt);
    /**
     * Whether or not GremlinQueryOptimizer should call this optimization recursively
     * on the updated children.
     */
    boolean isApplyRecursively();

    /**
     * Applies the optimization.
     *
     * @param expr
     * @param context
     * @return the optimized expression
     */
    GroovyExpression apply(GroovyExpression expr, OptimizationContext context);
}
