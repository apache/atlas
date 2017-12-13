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
import org.apache.atlas.type.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QueryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(QueryProcessor.class);

    private final int DEFAULT_QUERY_RESULT_LIMIT = 25;
    private final int DEFAULT_QUERY_RESULT_OFFSET = 0;

    private final boolean isNestedQuery;
    private final List<String>      errorList    = new ArrayList<>();
    private final GremlinClauseList queryClauses = new GremlinClauseList();
    private int providedLimit = DEFAULT_QUERY_RESULT_LIMIT;
    private int providedOffset = DEFAULT_QUERY_RESULT_OFFSET;
    private int currentStep;
    private final org.apache.atlas.query.Lookup lookup;
    private Context context;

    @Inject
    public QueryProcessor(AtlasTypeRegistry typeRegistry) {
        this.isNestedQuery = false;
        lookup = new Lookup(errorList, typeRegistry);
        context = new Context(errorList, lookup);
        init();
    }

    public QueryProcessor(AtlasTypeRegistry typeRegistry, int limit, int offset) {
        this(typeRegistry);
        this.providedLimit = limit;
        this.providedOffset = offset < 0 ? DEFAULT_QUERY_RESULT_OFFSET : offset;
    }

    @VisibleForTesting
    QueryProcessor(org.apache.atlas.query.Lookup lookup, Context context) {
        this.isNestedQuery = false;
        this.lookup = lookup;
        this.context = context;
        init();
    }

    public QueryProcessor(org.apache.atlas.query.Lookup registryLookup, boolean isNestedQuery) {
        this.isNestedQuery = isNestedQuery;
        this.lookup = registryLookup;
        init();
    }

    public Expression validate(Expression expression) {
        return expression.isReady();
    }

    private void init() {
        if (!isNestedQuery) {
            add(GremlinClause.G);
            add(GremlinClause.V);
        } else {
            add(GremlinClause.NESTED_START);
        }
    }

    public void addFrom(String typeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFrom(typeName={})", typeName);
        }

        IdentifierHelper.Advice ta = getAdvice(typeName);
        if(context.shouldRegister(ta.get())) {
            context.registerActive(ta.get());

            IdentifierHelper.Advice ia = getAdvice(ta.get());
            if (ia.isTrait()) {
                add(GremlinClause.TRAIT, ia.get());
            } else {
                if (ia.hasSubtypes()) {
                    add(GremlinClause.HAS_TYPE_WITHIN, ia.getSubTypes());
                } else {
                    add(GremlinClause.HAS_TYPE, ia.get());
                }
            }
        } else {
            IdentifierHelper.Advice ia = getAdvice(ta.get());
            introduceType(ia);
        }
    }

    private void introduceType(IdentifierHelper.Advice ia) {
        if (!ia.isPrimitive() && ia.getIntroduceType()) {
            add(GremlinClause.OUT, ia.getEdgeLabel());
            context.registerActive(ia.getTypeName());
        }
    }

    private IdentifierHelper.Advice getAdvice(String actualTypeName) {
        return IdentifierHelper.create(context, lookup, actualTypeName);
    }

    public void addFromProperty(String typeName, String attribute) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromProperty(typeName={}, attribute={})", typeName, attribute);
        }

        addFrom(typeName);
        add(GremlinClause.HAS_PROPERTY,
                IdentifierHelper.getQualifiedName(lookup, context, attribute));
    }


    public void addFromIsA(String typeName, String traitName) {
        addFrom(typeName);
        add(GremlinClause.TRAIT, traitName);
    }

    public void addWhere(String lhs, String operator, String rhs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addWhere(lhs={}, operator={}, rhs={})", lhs, operator, rhs);
        }

        String currentType  = context.getActiveTypeName();
        SearchParameters.Operator op = SearchParameters.Operator.fromString(operator);
        IdentifierHelper.Advice org = null;
        IdentifierHelper.Advice lhsI = getAdvice(lhs);
        if(lhsI.isPrimitive() == false) {
            introduceType(lhsI);
            org = lhsI;
            lhsI = getAdvice(lhs);
        }

        if(lhsI.isDate()) {
            rhs = parseDate(rhs);
        }

        rhs = addQuotesIfNecessary(rhs);
        if(op == SearchParameters.Operator.LIKE) {
            add(GremlinClause.TEXT_CONTAINS, lhsI.getQualifiedName(), rhs.replace("*", ".*").replace('?', '.'));
        } else if(op == SearchParameters.Operator.IN) {
            add(GremlinClause.HAS_OPERATOR, lhsI.getQualifiedName(), "within", rhs);
        } else {
            add(GremlinClause.HAS_OPERATOR, lhsI.getQualifiedName(), op.getSymbols()[1], rhs);
        }

        if(org != null && org.isPrimitive() == false && org.getIntroduceType()) {
            add(GremlinClause.IN, org.getEdgeLabel());
            context.registerActive(currentType);
        }
    }

    private String addQuotesIfNecessary(String rhs) {
        if(IdentifierHelper.isQuoted(rhs)) return rhs;
        return quoted(rhs);
    }

    private static String quoted(String rhs) {
        return IdentifierHelper.getQuoted(rhs);
    }

    private String parseDate(String rhs) {
        String s = IdentifierHelper.isQuoted(rhs) ?
                IdentifierHelper.removeQuotes(rhs) :
                rhs;
        return String.format("'%d'", DateTime.parse(s).getMillis());
    }

    public void addAndClauses(List<String> clauses) {
        queryClauses.add(GremlinClause.AND, StringUtils.join(clauses, ','));
    }

    public void addOrClauses(List<String> clauses) {
        queryClauses.add(GremlinClause.OR, StringUtils.join(clauses, ','));
    }

    public void addSelect(List<Pair<String, String>> items) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addSelect(items.length={})", items != null ? items.size() : -1);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.size(); i++) {
            IdentifierHelper.Advice ia = getAdvice(items.get(i).getLeft());
            if(StringUtils.isNotEmpty(items.get(i).getRight())) {
                context.aliasMap.put(items.get(i).getRight(), ia.getQualifiedName());
            }

            if(!ia.isPrimitive() && ia.getIntroduceType()) {
                add(GremlinClause.OUT, ia.getEdgeLabel());
                add(GremlinClause.AS, getCurrentStep());
                addSelectClause(getCurrentStep());
                incrementCurrentStep();
            }  else {
                sb.append(quoted(ia.getQualifiedName()));
            }

            if (i != items.size() - 1) {
                sb.append(",");
            }
        }

        if (!StringUtils.isEmpty(sb.toString())) {
            addValueMapClause(sb.toString());
        }
    }

    private void addSelectClause(String s) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addSelectClause(s={})", s);
        }

        add(GremlinClause.SELECT, s);
    }

    private String getCurrentStep() {
        return String.format("s%d", currentStep);
    }

    private void incrementCurrentStep() {
        currentStep++;
    }

    public QueryProcessor createNestedProcessor() {
        QueryProcessor qp = new QueryProcessor(lookup, true);
        qp.context = this.context;
        return qp;
    }

    private void addValueMapClause(String s) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addValueMapClause(s={})", s);
        }

        add(GremlinClause.VALUE_MAP, s);
    }

    public void addFromAlias(String typeName, String alias) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromAlias(typeName={}, alias={})", typeName, alias);
        }

        addFrom(typeName);
        addAsClause(alias);
        context.registerAlias(alias);
    }

    public void addAsClause(String stepName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addAsClause(stepName={})", stepName);
        }

        add(GremlinClause.AS, stepName);
    }

    private void add(GremlinClause clause, String... args) {
        queryClauses.add(new GremlinClauseValue(clause, clause.get(args)));
    }

    private void addRangeClause(String startIndex, String endIndex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addRangeClause(startIndex={}, endIndex={})", startIndex, endIndex);
        }

        add(GremlinClause.RANGE, startIndex, startIndex, endIndex);
    }


    public void addGroupBy(String item) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupBy(item={})", item);
        }

        add(GremlinClause.GROUP);
        addByClause(item, false);
    }

    private void addByClause(String name, boolean descr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addByClause(name={})", name, descr);
        }

        IdentifierHelper.Advice ia = getAdvice(name);
        add((!descr) ? GremlinClause.BY : GremlinClause.BY_DESC,
                ia.getQualifiedName());
    }

    public void addLimit(String limit, String offset) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addLimit(limit={}, offset={})", limit, offset);
        }

        if (offset.equalsIgnoreCase("0")) {
            add(GremlinClause.LIMIT, limit);
        } else {
            addRangeClause(offset, limit);
        }
    }

    public void close() {
        if (queryClauses.isEmpty()) {
            queryClauses.clear();
            return;
        }

        if (queryClauses.hasClause(GremlinClause.LIMIT) == -1) {
            addLimit(Integer.toString(providedLimit), Integer.toString(providedOffset));
        }

        add(GremlinClause.TO_LIST);
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

    public boolean hasSelect() {
        return (queryClauses.hasClause(GremlinClause.VALUE_MAP) != -1);
    }

    public void addOrderBy(String name, boolean isDesc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderBy(name={}, isDesc={})", name, isDesc);
        }

        add(GremlinClause.ORDER);
        addByClause(name, isDesc);
        updateSelectClausePosition();
    }

    private void updateSelectClausePosition() {
        int selectClauseIndex = queryClauses.hasClause(GremlinClause.VALUE_MAP);
        if(-1 == selectClauseIndex) {
            return;
        }

        GremlinClauseValue gcv = queryClauses.remove(selectClauseIndex);
        queryClauses.add(gcv);
    }

    private enum GremlinClause {
        AS("as('%s')"),
        BY("by('%s')"),
        BY_DESC("by('%s', decr)"),
        DEDUP("dedup()"),
        G("g"),
        GROUP("group()"),
        HAS("has('%s', %s)"),
        HAS_OPERATOR("has('%s', %s(%s))"),
        HAS_PROPERTY("has('%s')"),
        HAS_NOT_PROPERTY("hasNot('%s')"),
        HAS_TYPE("has('__typeName', '%s')"),
        HAS_TYPE_WITHIN("has('__typeName', within(%s))"),
        HAS_WITHIN("has('%s', within(%s))"),
        IN("in('%s')"),
        OR("or(%s)"),
        AND("and(%s)"),
        NESTED_START("__"),
        NESTED_HAS_OPERATOR("has('%s', %s(%s))"),
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
        private final List<GremlinClauseValue> list;

        private GremlinClauseList() {
            this.list = new LinkedList<>();
        }

        public void add(GremlinClauseValue g) {
            list.add(g);
        }

        public void add(GremlinClauseValue g, AtlasEntityType t) {
            add(g);
        }

        public void add(GremlinClause clause, String... args) {
            list.add(new GremlinClauseValue(clause, clause.get(args)));
        }

        public String getValue(int i) {
            return list.get(i).value;
        }

        public GremlinClauseValue get(int i) {
            return list.get(i);
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

        public boolean isEmpty() {
            return list.size() == 0 || list.size() == 2;
        }

        public void clear() {
            list.clear();
        }

        public GremlinClauseValue remove(int index) {
            GremlinClauseValue gcv = get(index);
            list.remove(index);
            return gcv;
        }
    }

    @VisibleForTesting
    static class Context {
        private final List<String> errorList;
        org.apache.atlas.query.Lookup lookup;
        private AtlasType activeType;
        Map<String, String> aliasMap = new HashMap<>();

        public Context(List<String> errorList, org.apache.atlas.query.Lookup lookup) {
            this.lookup = lookup;
            this.errorList = errorList;
        }

        public void registerActive(String typeName) {
            if(shouldRegister(typeName)) {
                activeType = lookup.getType(typeName);
            }

            aliasMap.put(typeName, typeName);
        }

        public AtlasType getActiveType() {
            return activeType;
        }

        public AtlasEntityType getActiveEntityType() {
            return (activeType instanceof AtlasEntityType) ?
                    (AtlasEntityType) activeType :
                    null;
        }

        public String getActiveTypeName() {
            return activeType.getTypeName();
        }

        public boolean shouldRegister(String typeName) {
            return activeType == null ||
                    (activeType != null && !StringUtils.equals(getActiveTypeName(), typeName)) &&
                            (activeType != null && !lookup.hasAttribute(this, typeName));
        }

        public void registerAlias(String alias) {
            if(aliasMap.containsKey(alias)) {
                errorList.add(String.format("Duplicate alias found: %s for type %s already present.", alias, getActiveEntityType()));
                return;
            }

            aliasMap.put(alias, getActiveTypeName());
        }

        public boolean hasAlias(String alias) {
            return aliasMap.containsKey(alias);
        }

        public String getTypeNameFromAlias(String alias) {
            return aliasMap.get(alias);
        }

        public boolean isEmpty() {
            return activeType == null;
        }
    }

    private static class Lookup implements org.apache.atlas.query.Lookup {
        private final List<String> errorList;
        private final AtlasTypeRegistry typeRegistry;

        public Lookup(List<String> errorList, AtlasTypeRegistry typeRegistry) {
            this.errorList = errorList;
            this.typeRegistry = typeRegistry;
        }

        @Override
        public AtlasType getType(String typeName) {
            try {
                return typeRegistry.getType(typeName);
            } catch (AtlasBaseException e) {
                addError(e.getMessage());
            }

            return null;
        }

        @Override
        public String getQualifiedName(Context context, String name) {
            try {
                AtlasEntityType et = context.getActiveEntityType();
                if(et == null) {
                    return "";
                }

                return et.getQualifiedAttributeName(name);
            } catch (AtlasBaseException e) {
                addError(e.getMessage());
            }

            return "";
        }

        protected void addError(String s) {
            errorList.add(s);
        }

        @Override
        public boolean isPrimitive(Context context, String attributeName) {
            AtlasEntityType et = context.getActiveEntityType();
            if(et == null) {
                return false;
            }

            AtlasType attr = et.getAttributeType(attributeName);
            if(attr == null) {
                return false;
            }

            TypeCategory attrTypeCategory = attr.getTypeCategory();
            return (attrTypeCategory != null) && (attrTypeCategory == TypeCategory.PRIMITIVE || attrTypeCategory == TypeCategory.ENUM);
        }

        @Override
        public String getRelationshipEdgeLabel(Context context, String attributeName) {
            AtlasEntityType et = context.getActiveEntityType();
            if(et == null) {
                return "";
            }

            AtlasStructType.AtlasAttribute attr = et.getAttribute(attributeName);
            return (attr != null) ? attr.getRelationshipEdgeLabel() : "";
        }

        @Override
        public boolean hasAttribute(Context context, String typeName) {
            return (context.getActiveEntityType() != null) && context.getActiveEntityType().getAttribute(typeName) != null;
        }

        @Override
        public boolean doesTypeHaveSubTypes(Context context) {
            return (context.getActiveEntityType() != null && context.getActiveEntityType().getAllSubTypes().size() > 0);
        }

        @Override
        public String getTypeAndSubTypes(Context context) {
            String[] str = context.getActiveEntityType() != null ?
                            context.getActiveEntityType().getTypeAndAllSubTypes().toArray(new String[]{}) :
                            new String[]{};
            if(str.length == 0) {
                return null;
            }

            String[] quoted = new String[str.length];
            for (int i = 0; i < str.length; i++) {
                quoted[i] = quoted(str[i]);
            }

            return StringUtils.join(quoted, ",");
        }

        @Override
        public boolean isTraitType(Context context) {
            return (context.getActiveType() != null &&
                    context.getActiveType().getTypeCategory() == TypeCategory.CLASSIFICATION);
        }

        @Override
        public String getTypeFromEdge(Context context, String item) {
            AtlasEntityType et = context.getActiveEntityType();
            if(et == null) {
                return "";
            }

            AtlasStructType.AtlasAttribute attr = et.getAttribute(item);
            if(attr == null) {
                return null;
            }

            AtlasType at = attr.getAttributeType();
            if(at.getTypeCategory() == TypeCategory.ARRAY) {
                AtlasArrayType arrType = ((AtlasArrayType)at);
                return ((AtlasBuiltInTypes.AtlasObjectIdType) arrType.getElementType()).getObjectType();
            }

            return context.getActiveEntityType().getAttribute(item).getTypeName();
        }

        @Override
        public boolean isDate(Context context, String attributeName) {
            AtlasEntityType et = context.getActiveEntityType();
            if(et == null) {
                return false;
            }

            AtlasType attr = et.getAttributeType(attributeName);
            if(attr == null) {
                return false;
            }

            return attr.getTypeName().equals("date");
        }
    }
}
