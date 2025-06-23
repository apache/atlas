/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.atlas.model.AtlasBaseModelObject;
import org.apache.atlas.model.annotation.AtlasJSON;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.dumpDateField;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.dumpObjects;

@AtlasJSON
public class AtlasRule extends AtlasBaseModelObject implements Serializable {
    private static final long serialVersionUID = 1L;

    public  String   desc;
    public  String   action;
    public  String   ruleName;
    public  RuleExpr ruleExpr;
    private Date     createdTime;
    private Date     updatedTime;

    public AtlasRule() {
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public RuleExpr getRuleExpr() {
        return ruleExpr;
    }

    public void setRuleExpr(RuleExpr ruleExpr) {
        this.ruleExpr = ruleExpr;
    }

    public Date getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTime = createdTime;
    }

    public Date getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(Date updatedTime) {
        this.updatedTime = updatedTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), desc, action, ruleName, ruleExpr, createdTime, updatedTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        AtlasRule atlasRule = (AtlasRule) o;
        return Objects.equals(desc, atlasRule.desc) && Objects.equals(action, atlasRule.action) && Objects.equals(ruleName, atlasRule.ruleName) && Objects.equals(ruleExpr, atlasRule.ruleExpr) && Objects.equals(createdTime, atlasRule.createdTime) && Objects.equals(updatedTime, atlasRule.updatedTime);
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }
        sb.append(", ruleName=").append(ruleName);
        sb.append(", desc=").append(desc);
        sb.append(", action=").append(action);
        sb.append(", ruleExpr=").append(ruleExpr);
        dumpDateField(", createdTime=", createdTime, sb);
        dumpDateField(", updatedTime=", updatedTime, sb);

        return sb;
    }

    public enum Condition { AND, OR}

    public static class RuleExpr {
        public List<RuleExprObject> ruleExprObjList;

        public RuleExpr() {
        }

        public RuleExpr(List<RuleExprObject> ruleExprObjList) {
            this.ruleExprObjList = ruleExprObjList;
        }

        public List<RuleExprObject> getRuleExprObjList() {
            return ruleExprObjList;
        }

        public void setRuleExprObjList(List<RuleExprObject> ruleExprObjList) {
            this.ruleExprObjList = ruleExprObjList;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ruleExprObjList);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RuleExpr ruleExpr = (RuleExpr) o;
            return Objects.equals(ruleExprObjList, ruleExpr.ruleExprObjList);
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("{\"ruleExprObjList\":[");
            dumpObjects(ruleExprObjList, sb);
            sb.append("]}");
            return sb;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RuleExprObject {
        public String          typeName;
        public Condition       condition;
        public List<Criterion> criterion;
        public String          attributeName;
        public Operator        operator;
        public String          attributeValue;
        public boolean         includeSubTypes;

        public RuleExprObject() {
        }

        public RuleExprObject(String typeName, String attributeName, Operator operator, String attributeValue, boolean includeSubTypes) {
            this.typeName        = typeName;
            this.includeSubTypes = includeSubTypes;
            this.attributeName   = attributeName;
            this.operator        = operator;
            this.attributeValue  = attributeValue;
        }

        public RuleExprObject(String typeName, Condition condition, List<Criterion> criterion, boolean includeSubTypes) {
            this.typeName        = typeName;
            this.includeSubTypes = includeSubTypes;
            this.condition       = condition;
            this.criterion       = criterion;
        }

        public RuleExprObject(String typeName, boolean includeSubTypes) {
            this.typeName        = typeName;
            this.includeSubTypes = includeSubTypes;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public boolean getIncludeSubTypes() {
            return includeSubTypes;
        }

        public void setIncludeSubTypes(boolean includeSubTypes) {
            this.includeSubTypes = includeSubTypes;
        }

        public Condition getCondition() {
            return condition;
        }

        public void setCondition(Condition condition) {
            this.condition = condition;
        }

        public List<Criterion> getCriterion() {
            return criterion;
        }

        public void setCriterion(List<Criterion> criterion) {
            this.criterion = criterion;
        }

        public String getAttributeName() {
            return attributeName;
        }

        public void setAttributeName(String attributeName) {
            this.attributeName = attributeName;
        }

        public Operator getOperator() {
            return operator;
        }

        public void setOperator(Operator operator) {
            this.operator = operator;
        }

        public String getAttributeValue() {
            return attributeValue;
        }

        public void setAttributeValue(String attributeValue) {
            this.attributeValue = attributeValue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(typeName, condition, criterion, attributeName, operator, attributeValue, includeSubTypes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RuleExprObject that = (RuleExprObject) o;
            return includeSubTypes == that.includeSubTypes && Objects.equals(typeName, that.typeName) && condition == that.condition && Objects.equals(criterion, that.criterion) && Objects.equals(attributeName, that.attributeName) && operator == that.operator && Objects.equals(attributeValue, that.attributeValue);
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }
            sb.append('{');
            if (typeName != null) {
                sb.append("\"typeName\":\"").append(typeName).append('"');
            }
            if (attributeName != null) {
                sb.append(",\"attributeName\":\"").append(attributeName).append('"');
            }
            if (operator != null) {
                sb.append(",\"operator\":\"").append(operator).append('"');
            }
            if (attributeValue != null) {
                sb.append(",\"attributeValue\":\"").append(attributeValue).append('"');
            }
            if (condition != null) {
                sb.append(",\"condition\":").append(condition);
            }
            if (criterion != null) {
                sb.append(", \"criterion\":").append(criterion);
            }
            sb.append(", includeSubTypes='").append(includeSubTypes).append('\'');
            sb.append('}');

            return sb;
        }

        public enum Operator {
            LT("<"),
            GT(">"),
            LTE("<="),
            GTE(">="),
            EQ("=="),
            NEQ("!="),
            STARTS_WITH("startsWith"),
            ENDS_WITH("endsWith"),
            CONTAINS("contains"),
            NOT_CONTAINS("notContains"),
            CONTAINS_IGNORECASE("containsIgnoreCase"),
            NOT_CONTAINS_IGNORECASE("notContainsIgnoreCase"),
            IS_NULL(OperatorType.UNARY, "isNull"),
            NOT_NULL(OperatorType.UNARY, "notNull");

            private final String       symbol;
            private       OperatorType type = OperatorType.BINARY;
            Operator(String symbol) {
                this.symbol = symbol;
            }

            Operator(OperatorType type, String symbol) {
                this.symbol = symbol;
                this.type   = type;
            }

            @JsonValue
            public String getSymbol() {
                return symbol;
            }

            public boolean isBinary() {
                return type == OperatorType.BINARY;
            }

            @Override
            public String toString() {
                return getSymbol();
            }

            enum TYPE {
                UNARY,
                BINARY
            }
        }

        public enum OperatorType {
            UNARY("Unary"), BINARY("Binary");
            private final String displayName;

            OperatorType(final String displayName) {
                this.displayName = displayName;
            }

            public String getDisplayName() {
                return displayName;
            }
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Criterion {
            public Operator        operator;
            public String          attributeName;
            public String          attributeValue;
            public Condition       condition;
            public List<Criterion> criterion;

            public Criterion() {
            }

            public Criterion(Operator operator, String attributeName, String attributeValue) {
                this.operator       = operator;
                this.attributeName  = attributeName;
                this.attributeValue = attributeValue;
            }

            public Criterion(Condition condition, List<Criterion> criterion) {
                this.condition = condition;
                this.criterion = criterion;
            }

            public Operator getOperator() {
                return operator;
            }

            public void setOperator(Operator operator) {
                this.operator = operator;
            }

            public String getAttributeName() {
                return attributeName;
            }

            public void setAttributeName(String attributeName) {
                this.attributeName = attributeName;
            }

            public String getAttributeValue() {
                return attributeValue;
            }

            public void setAttributeValue(String attributeValue) {
                this.attributeValue = attributeValue;
            }

            public Condition getCondition() {
                return condition;
            }

            public void setCondition(Condition condition) {
                this.condition = condition;
            }

            public List<Criterion> getCriterion() {
                return criterion;
            }

            public void setCriterion(List<Criterion> criterion) {
                this.criterion = criterion;
            }

            @Override
            public int hashCode() {
                return Objects.hash(operator, attributeName, attributeValue, condition, criterion);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Criterion criterion1 = (Criterion) o;
                return operator == criterion1.operator && Objects.equals(attributeName, criterion1.attributeName) && Objects.equals(attributeValue, criterion1.attributeValue) && condition == criterion1.condition && Objects.equals(criterion, criterion1.criterion);
            }

            @Override
            public String toString() {
                return toString(new StringBuilder()).toString();
            }

            public StringBuilder toString(StringBuilder sb) {
                if (sb == null) {
                    sb = new StringBuilder();
                }

                sb.append('{');
                if (attributeName != null) {
                    sb.append("\"attributeName\":\"").append(attributeName).append('"');
                }
                if (operator != null) {
                    sb.append(",\"operator\":\"").append(operator).append('"');
                }
                if (attributeValue != null) {
                    sb.append(",\"attributeValue\":\"").append(attributeValue).append('"');
                }
                if (condition != null) {
                    sb.append(" \"condition\":").append(condition);
                }
                if (criterion != null) {
                    sb.append(", \"criterion\":").append(criterion);
                }
                sb.append('}');

                return sb;
            }
        }
    }
}
