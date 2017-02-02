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

package org.apache.atlas.groovy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Groovy expression that calls a method on an object.
 */
public class FunctionCallExpression extends AbstractGroovyExpression {

    // null for global functions
    private GroovyExpression target;

    private String functionName;
    private List<GroovyExpression> arguments = new ArrayList<>();

    public FunctionCallExpression(String functionName, List<? extends GroovyExpression> arguments) {
        this.target = null;
        this.functionName = functionName;
        this.arguments.addAll(arguments);
    }

    public FunctionCallExpression(GroovyExpression target, String functionName,
            List<? extends GroovyExpression> arguments) {
        this.target = target;
        this.functionName = functionName;
        this.arguments.addAll(arguments);
    }

    public FunctionCallExpression(String functionName, GroovyExpression... arguments) {
        this.target = null;
        this.functionName = functionName;
        this.arguments.addAll(Arrays.asList(arguments));
    }

    public FunctionCallExpression(GroovyExpression target, String functionName, GroovyExpression... arguments) {
        this.target = target;
        this.functionName = functionName;
        this.arguments.addAll(Arrays.asList(arguments));
    }

    public void addArgument(GroovyExpression expr) {
        arguments.add(expr);
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {

        if (target != null) {
            target.generateGroovy(context);
            context.append(".");
        }
        context.append(functionName);
        context.append("(");
        Iterator<GroovyExpression> it = arguments.iterator();
        while (it.hasNext()) {
            GroovyExpression expr = it.next();
            expr.generateGroovy(context);
            if (it.hasNext()) {
                context.append(", ");
            }
        }
        context.append(")");
    }

}
