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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.ClosureExpression;
import org.apache.atlas.groovy.ClosureExpression.VariableDeclaration;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.ListExpression;
import org.apache.atlas.groovy.TypeCoersionExpression;
import org.apache.atlas.groovy.VariableAssignmentExpression;

/**
 * Maintains state information during gremlin optimization.
 */
public class OptimizationContext {

    private static final String TMP_ALIAS_NAME = "__tmp";
    private static final String FINAL_ALIAS_NAME = "__res";
    private static final String RESULT_VARIABLE = "r";
    private final List<GroovyExpression> initialStatements = new ArrayList<>();
    private GroovyExpression resultExpression = getResultVariable();
    private int counter = 1;
    private final Map<String, ClosureExpression> functionBodies = new HashMap<>();
    private AbstractFunctionExpression rangeExpression;

    public OptimizationContext() {

    }

    /**
     * @return
     */
    public List<GroovyExpression> getInitialStatements() {
        return initialStatements;
    }

    public void prependStatement(GroovyExpression expr) {
        initialStatements.add(0, expr);
    }

    public String getUniqueFunctionName() {
        return "f" + (counter++);
    }


    public GroovyExpression getDefineResultVariableStmt() {
        GroovyExpression castExpression = new TypeCoersionExpression(new ListExpression(), "Set");
        GroovyExpression resultVarDef =  new VariableAssignmentExpression(RESULT_VARIABLE, castExpression);
        return resultVarDef;

    }
    public void setResultExpression(GroovyExpression expr) {
        resultExpression = expr;
    }

    public GroovyExpression getResultExpression() {
        return resultExpression;
    }

    public GroovyExpression getResultVariable() {
        return new IdentifierExpression(RESULT_VARIABLE);
    }

    public ClosureExpression getUserDefinedFunctionBody(String functionName) {
        return functionBodies.get(functionName);
    }

    public String addFunctionDefinition(VariableDeclaration decl, GroovyExpression body) {
        String functionName = getUniqueFunctionName();
        List<VariableDeclaration> decls = (decl == null) ? Collections.<VariableDeclaration>emptyList() : Collections.singletonList(decl);
        ClosureExpression bodyClosure = new ClosureExpression(body, decls);
        VariableAssignmentExpression expr = new VariableAssignmentExpression(functionName, bodyClosure);
        initialStatements.add(expr);
        functionBodies.put(functionName, bodyClosure);
        return functionName;
    }

    public String getFinalAliasName() {
        return FINAL_ALIAS_NAME;
    }

    public String getTempAliasName() {
        return TMP_ALIAS_NAME;
    }

    public void setRangeExpression(AbstractFunctionExpression rangeExpression) {
        this.rangeExpression = rangeExpression;
    }

    public AbstractFunctionExpression getRangeExpression() {
        return rangeExpression;
    }
}
