/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.query;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.query.Expressions.Expression;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasBuiltInTypes;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(QueryProcessor.class);

    private final int DEFAULT_QUERY_RESULT_LIMIT = 25;

    private final Pattern SINGLE_QUOTED_IDENTIFIER   = Pattern.compile("'(\\w[\\w\\d\\.\\s]*)'");
    private final Pattern DOUBLE_QUOTED_IDENTIFIER   = Pattern.compile("\"(\\w[\\w\\d\\.\\s]*)\"");
    private final Pattern BACKTICK_QUOTED_IDENTIFIER = Pattern.compile("`(\\w[\\w\\d\\.\\s]*)`");

    private final List<String> errorList         = new ArrayList<>();
    private final GremlinClauseList queryClauses = new GremlinClauseList(errorList);
    private int currentStep;
    private final TypeRegistryLookup registryLookup;

    @Inject
    public QueryProcessor(AtlasTypeRegistry typeRegistry) {
        registryLookup = new TypeRegistryLookup(errorList, typeRegistry);
        init();
    }

    @VisibleForTesting
    public QueryProcessor(TypeRegistryLookup lookup) {
        registryLookup = lookup;
        init();
    }

    private void init() {
        add(GremlinClause.G);
        add(GremlinClause.V);
    }

    public Expression validate(Expression expression) {
        return expression.isReady();
    }

    public void addFrom(String typeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFrom(typeName={})", typeName);
        }

        String actualTypeName = extractIdentifier(typeName);

        if(registryLookup.isTypeTrait(actualTypeName)) {
            addTraitAndRegister(actualTypeName);
        } else if (!registryLookup.hasActiveType()) {
            registryLookup.registerActive(actualTypeName);
            if(registryLookup.doesActiveTypeHaveSubTypes()) {
                add(GremlinClause.HAS_TYPE_WITHIN, registryLookup.getActiveTypeAndSubTypes());
            } else {
                add(GremlinClause.HAS_TYPE, actualTypeName);
            }
        } else {
            add(GremlinClause.OUT, registryLookup.getRelationshipEdgeLabelForActiveType(actualTypeName));
            registryLookup.registerActive(registryLookup.getTypeFromEdge(actualTypeName));
        }
    }

    private void addTraitAndRegister(String typeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addTraitAndRegister(typeName={})", typeName);
        }

        add(GremlinClause.TRAIT, typeName);
        registryLookup.registerActive(typeName);
    }

    public void addFromIsA(String typeName, String trait) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromIsA(typeName={}, trait={})", typeName, trait);
        }

        if(!registryLookup.hasActiveType()) {
            addFrom(typeName);
        }

        add(GremlinClause.TRAIT, trait);
    }

    public void addFromProperty(String typeName, String attribute) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromIsA(typeName={}, attribute={})", typeName, attribute);
        }

        if(registryLookup.isSameAsActive(typeName) == false) {
            addFrom(typeName);
        }

        add(GremlinClause.HAS_PROPERTY, registryLookup.getQualifiedAttributeName(attribute));
    }

    public void addFromAlias(String typeName, String alias) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromAlias(typeName={}, alias={})", typeName, alias);
        }

        addFrom(typeName);
        addAsClause(alias);
    }

    public void addWhere(String lhs, String operator, String rhs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addWhere(lhs={}, operator={}, rhs={})", lhs, operator, rhs);
        }

        lhs = registryLookup.getQualifiedAttributeName(lhs);

        SearchParameters.Operator op = SearchParameters.Operator.fromString(operator);
        switch (op) {
            case LT:
                add(GremlinClause.HAS_OPERATOR, lhs, "lt", rhs);
                break;
            case GT:
                add(GremlinClause.HAS_OPERATOR, lhs, "gt", rhs);
                break;
            case LTE:
                add(GremlinClause.HAS_OPERATOR, lhs, "lte", rhs);
                break;
            case GTE:
                add(GremlinClause.HAS_OPERATOR, lhs, "gte", rhs);
                break;
            case EQ:
                add(GremlinClause.HAS_OPERATOR, lhs, "eq", rhs);
                break;
            case NEQ:
                add(GremlinClause.HAS_OPERATOR, lhs, "neq", rhs);
                break;
            case IN:
                // TODO: Handle multiple RHS values
                add(GremlinClause.HAS_OPERATOR, lhs, "within", rhs);
                break;
            case LIKE:
                add(GremlinClause.TEXT_CONTAINS, lhs, rhs.replace("*", ".*").replace('?', '.'));
                break;
        }
    }

    public void addSelect(String[] items) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addSelect(items.length={})", items != null ? items.length : -1);
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < items.length; i++) {
            String s = registryLookup.getQualifiedAttributeName(items[i]);

            if (items[i].contains(".") || registryLookup.isAttributePrimitiveTypeForActiveType(items[i])) {
                sb.append(String.format("'%s'", s));

                if (i != items.length - 1) {
                    sb.append(", ");
                }
            } else {
                add(GremlinClause.OUT, registryLookup.getRelationshipEdgeLabelForActiveType(items[i]));
                add(GremlinClause.AS, getCurrentStep());
                addSelectClause(getCurrentStep());
                incrementCurrentStep();
            }
        }

        if (!StringUtils.isEmpty(sb.toString())) {
            addValueMapClause(sb.toString());
        }
    }

    public void addLimit(String limit, String offset) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addLimit(limit={}, offset={})", limit, offset);
        }

        add(GremlinClause.ORDER);

        if (offset.equalsIgnoreCase("0")) {
            add(GremlinClause.LIMIT, limit);
        } else {
            addRangeClause(offset, limit);
        }
    }

    public void addGroupBy(String item) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupBy(item={})", item);
        }

        add(GremlinClause.GROUP);
        addByClause(item, false);
    }

    private void addRangeClause(String startIndex, String endIndex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addRangeClause(startIndex={}, endIndex={})", startIndex, endIndex);
        }

        add(GremlinClause.RANGE, startIndex, startIndex, endIndex);
    }

    public String getText() {
        String[] items = new String[queryClauses.size()];

        for (int i = 0; i < queryClauses.size(); i++) {
            items[i] = queryClauses.getValue(i);
        }

        String ret = StringUtils.join(items, ".");

        if (LOG.isDebugEnabled()) {
            LOG.debug("getText() => {}", ret);
        }

        return ret;
    }

    public void close() {
        if(queryClauses.hasClause(GremlinClause.LIMIT) == -1) {
            add(GremlinClause.LIMIT, "" + DEFAULT_QUERY_RESULT_LIMIT);
        }
        add(GremlinClause.TO_LIST);
    }

    public boolean hasSelect() {
        return (queryClauses.hasClause(GremlinClause.VALUE_MAP) != -1);
    }

    public void addAsClause(String stepName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addAsClause(stepName={})", stepName);
        }

        add(GremlinClause.AS, stepName);
        registryLookup.registerStepType(stepName);
    }

    public void addOrderBy(String name, boolean isDesc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderBy(name={}, isDesc={})", name, isDesc);
        }

        add(GremlinClause.ORDER);
        addByClause(registryLookup.getQualifiedAttributeName(name), isDesc);
    }

    private void addValueMapClause(String s) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addValueMapClause(s={})", s);
        }

        add(GremlinClause.VALUE_MAP, s);
    }

    private void addSelectClause(String s) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addSelectClause(s={})", s);
        }

        add(GremlinClause.SELECT, s);
    }

    private void addByClause(String name, boolean descr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addByClause(name={})", name, descr);
        }

        add((!descr) ? GremlinClause.BY : GremlinClause.BY_DESC,
                registryLookup.getQualifiedAttributeName(name));
    }

    private String getCurrentStep() {
        return String.format("s%d", currentStep);
    }

    private void incrementCurrentStep() {
        currentStep++;
    }

    private void add(GremlinClause clause, String... args) {
        queryClauses.add(new GremlinClauseValue(clause, clause.get(args)));
    }

    private String extractIdentifier(String quotedIdentifier) {
        String ret;

        if (quotedIdentifier.charAt(0) == '`') {
            ret = extract(BACKTICK_QUOTED_IDENTIFIER, quotedIdentifier);
        } else if (quotedIdentifier.charAt(0) == '\'') {
            ret = extract(SINGLE_QUOTED_IDENTIFIER, quotedIdentifier);
        } else if (quotedIdentifier.charAt(0) == '"') {
            ret = extract(DOUBLE_QUOTED_IDENTIFIER, quotedIdentifier);
        } else {
            ret = quotedIdentifier;
        }

        return ret;
    }

    private String extract(Pattern p, String s) {
        Matcher m = p.matcher(s);
        return m.find() ? m.group(1) : s;
    }

    private enum GremlinClause {
        AS("as('%s')"),
        BY("by('%s')"),
        BY_DESC("by('%s', decr)"),
        G("g"),
        GROUP("group()"),
        HAS("has('%s', %s)"),
        HAS_OPERATOR("has('%s', %s(%s))"),
        HAS_PROPERTY("has('%s')"),
        HAS_NOT_PROPERTY("hasNot('%s')"),
        HAS_TYPE("has('__typeName', '%s')"),
        HAS_TYPE_WITHIN("has('__typeName', within(%s))"),
        HAS_WITHIN("has('%s', within(%s))"),
        IN("in()"),
        LIMIT("limit(%s)"),
        ORDER("order()"),
        OUT("out('%s')"),
        RANGE("range(%s, %s + %s)"),
        SELECT("select('%s')"),
        TO_LIST("toList()"),
        TEXT_CONTAINS("has('%s', org.janusgraph.core.attribute.Text.textRegex(%s))"),
        TEXT_PREFIX("has('%s', org.janusgraph.core.attribute.Text.textPrefix(%s))"),
        TEXT_SUFFIX("has('%s', org.janusgraph.core.attribute.Text.textRegex(\".*\" + %s))"),
        TRAIT("has('__traitNames', within('%s'))"),
        V("V()"),
        VALUE_MAP("valueMap(%s)");

        private final String format;

        GremlinClause(String format) {
            this.format = format;
        }

        String get(String... args) {
            return (args == null || args.length == 0) ?
                    format :
                    String.format(format, args);
        }
    }

    private static class GremlinClauseValue {
        private final GremlinClause clause;
        private final String value;

        public GremlinClauseValue(GremlinClause clause, String value) {
            this.clause = clause;
            this.value = value;
        }

        public GremlinClause getClause() {
            return clause;
        }

        public String getValue() {
            return value;
        }
    }

    private static class GremlinClauseList {
        private final List<String> errorList;
        private AtlasEntityType activeType;

        private final List<GremlinClauseValue> list;

        private GremlinClauseList(List<String> errorList) {
            this.errorList = errorList;
            this.list = new LinkedList<>();
        }

        public void add(GremlinClauseValue g) {
            list.add(g);
        }

        public void add(GremlinClauseValue g, AtlasEntityType t) {
            add(g);
            activeType = t;
        }

        public void add(GremlinClause clause, String... args) {
            list.add(new GremlinClauseValue(clause, clause.get(args)));
        }

        public String getValue(int i) {
            return list.get(i).value;
        }

        public int size() {
            return list.size();
        }

        public int hasClause(GremlinClause clause) {
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getClause() == clause)
                    return i;
            }

            return -1;
        }
    }

    @VisibleForTesting
    static class TypeRegistryLookup {
        private final List<String> errorList;
        private final AtlasTypeRegistry typeRegistry;

        private AtlasEntityType activeType;
        private final Map<String, AtlasEntityType> asClauseContext = new HashMap<>();

        public TypeRegistryLookup(List<String> errorList, AtlasTypeRegistry typeRegistry) {
            this.errorList = errorList;
            this.typeRegistry = typeRegistry;
        }

        public void registerActive(String typeName) {
            activeType = typeRegistry.getEntityTypeByName(typeName);
        }

        public boolean hasActiveType() {
            return (activeType != null);
        }

        public void registerStepType(String stepName) {
            if (!asClauseContext.containsKey(stepName)) {
                asClauseContext.put(stepName, activeType);
            } else {
                addError(String.format("Multiple steps with same name detected: %s", stepName));
            }
        }

        protected void addError(String s) {
            errorList.add(s);
        }

        public String getRelationshipEdgeLabelForActiveType(String item) {
            return getRelationshipEdgeLabel(activeType, item);
        }

        private String getRelationshipEdgeLabel(AtlasEntityType t, String item) {
            if(t == null) {
                return "";
            }

            AtlasStructType.AtlasAttribute attr = t.getAttribute(item);
            return (attr != null) ? attr.getRelationshipEdgeLabel() : "";
        }

        protected boolean isAttributePrimitiveTypeForActiveType(String name) {
            return isAttributePrimitiveType(activeType, name);
        }

        private boolean isAttributePrimitiveType(AtlasEntityType t, String name) {
            if (activeType == null) {
                return false;
            }

            AtlasType attrType = t.getAttributeType(name);
            TypeCategory attrTypeCategory = attrType.getTypeCategory();

            return (attrTypeCategory == TypeCategory.PRIMITIVE || attrTypeCategory == TypeCategory.ENUM);
        }

        public boolean isTypeTrait(String name) {
            return (typeRegistry.getClassificationTypeByName(name) != null);
        }

        public String getQualifiedAttributeName(String item) {
            if (item.contains(".")) {
                String[] keyValue = StringUtils.split(item, ".");

                if (!asClauseContext.containsKey(keyValue[0])) {
                    return item;
                } else {
                    String s = getStitchedString(keyValue, 1, keyValue.length - 1);
                    return getQualifiedAttributeNameFromType(
                            asClauseContext.get(keyValue[0]), s);
                }
            }

            return getQualifiedAttributeNameFromType(activeType, item);
        }

        protected String getStitchedString(String[] keyValue, int startIndex, int endIndex) {
            if(startIndex == endIndex) {
                return keyValue[startIndex];
            }

            return StringUtils.join(keyValue, ".", startIndex, endIndex);
        }

        private String getQualifiedAttributeNameFromType(AtlasEntityType t, String item) {
            try {
                return (t != null) ? t.getQualifiedAttributeName(item) : item;
            } catch (AtlasBaseException e) {
                addError(e.getMessage());
            }

            return item;
        }

        public String getTypeFromEdge(String item) {
            AtlasType at = activeType.getAttribute(item).getAttributeType();
            if(at.getTypeCategory() == TypeCategory.ARRAY) {
                AtlasArrayType arrType = ((AtlasArrayType)at);
                return ((AtlasBuiltInTypes.AtlasObjectIdType) arrType.getElementType()).getObjectType();
            }

            return activeType.getAttribute(item).getTypeName();
        }

        public boolean doesActiveTypeHaveSubTypes() {
            return (activeType.getAllSubTypes().size() != 0);
        }

        public String getActiveTypeAndSubTypes() {
            Set<String> set = activeType.getTypeAndAllSubTypes();
            String[] str = set.toArray(new String[]{});
            for (int i = 0; i < str.length; i++) {
                str[i] = String.format("'%s'", str[i]);
            }

            return StringUtils.join(str, ",");
        }

        public boolean isSameAsActive(String typeName) {
            return (activeType != null) && activeType.getTypeName().equalsIgnoreCase(typeName);
        }
    }
}
