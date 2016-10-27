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
import java.util.Iterator;
import java.util.List;

/**
 * Groovy expression that represents a block of code
 * that contains 0 or more statements that are delimited
 * by semicolons.
 */
public class CodeBlockExpression extends AbstractGroovyExpression {

    private List<GroovyExpression> body = new ArrayList<>();

    public void addStatement(GroovyExpression expr) {
        body.add(expr);
    }

    public void addStatements(List<GroovyExpression> exprs) {
        body.addAll(exprs);
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {

        /*
         * the L:{} represents a groovy code block; the label is needed
         * to distinguish it from a groovy closure.
         */
        context.append("L:{");
        Iterator<GroovyExpression> stmtIt = body.iterator();
        while(stmtIt.hasNext()) {
            GroovyExpression stmt = stmtIt.next();
            stmt.generateGroovy(context);
            if (stmtIt.hasNext()) {
                context.append(";");
            }
        }
        context.append("}");

    }
}
