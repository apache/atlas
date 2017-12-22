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
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.query.Expressions.Expression;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasBuiltInTypes;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Stream;

public class QueryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(QueryProcessor.class);

    private final int DEFAULT_QUERY_RESULT_LIMIT = 25;
    private final int DEFAULT_QUERY_RESULT_OFFSET = 0;

    private final List<String>      errorList      = new ArrayList<>();
    private final GremlinClauseList queryClauses   = new GremlinClauseList();

    private int     providedLimit  = DEFAULT_QUERY_RESULT_LIMIT;
    private int     providedOffset = DEFAULT_QUERY_RESULT_OFFSET;
    private boolean hasSelect      = false;
    private boolean isSelectNoop   = false;
    private boolean hasGrpBy       = false;

    private final org.apache.atlas.query.Lookup lookup;
    private final boolean isNestedQuery;
    private int currentStep;
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

    public void addSelect(SelectExprMetadata selectExprMetadata) {
        String[] items  = selectExprMetadata.getItems();
        String[] labels = selectExprMetadata.getLabels();
        if (LOG.isDebugEnabled()) {
            LOG.debug("addSelect(items.length={})", items != null ? items.length : 0);
        }

        if (items != null) {
            for (int i = 0; i < items.length; i++) {
                IdentifierHelper.Advice ia = getAdvice(items[i]);

                if(!labels[i].equals(items[i])) {
                    context.aliasMap.put(labels[i], ia.getQualifiedName());
                }

                if (i == selectExprMetadata.getCountIdx()) {
                    items[i] = GremlinClause.INLINE_COUNT.get();
                } else if (i == selectExprMetadata.getMinIdx()) {
                    items[i] = GremlinClause.INLINE_MIN.get(ia.getQualifiedName(), ia.getQualifiedName());
                } else if (i == selectExprMetadata.getMaxIdx()) {
                    items[i] = GremlinClause.INLINE_MAX.get(ia.getQualifiedName(), ia.getQualifiedName());
                } else if (i == selectExprMetadata.getSumIdx()) {
                    items[i] = GremlinClause.INLINE_SUM.get(ia.getQualifiedName(), ia.getQualifiedName());
                } else {
                    if (!ia.isPrimitive() && ia.getIntroduceType()) {
                        add(GremlinClause.OUT, ia.getEdgeLabel());
                        context.registerActive(ia.getTypeName());

                        int dotIdx = ia.get().indexOf(".");
                        if (dotIdx != -1) {
                            IdentifierHelper.Advice iax = getAdvice(ia.get());
                            items[i] = GremlinClause.INLINE_GET_PROPERTY.get(iax.getQualifiedName());
                        } else {
                            isSelectNoop = true;
                        }
                    } else {
                        items[i] = GremlinClause.INLINE_GET_PROPERTY.get(ia.getQualifiedName());
                    }
                }
            }

            // If GroupBy clause exists then the query spits out a List<Map<String, List<AtlasVertex>>> otherwise the query returns List<AtlasVertex>
            // Different transformations are needed for DSLs with groupby and w/o groupby
            GremlinClause transformationFn;
            if (isSelectNoop) {
                transformationFn = GremlinClause.SELECT_EXPR_NOOP_FN;
            } else {
                transformationFn = hasGrpBy ? GremlinClause.SELECT_WITH_GRPBY_HELPER_FN : GremlinClause.SELECT_EXPR_HELPER_FN;
            }
            queryClauses.add(0, transformationFn, getJoinedQuotedStr(labels), String.join(",", items));
            queryClauses.add(GremlinClause.INLINE_TRANSFORM_CALL);

            hasSelect = true;
        }
    }

    public QueryProcessor createNestedProcessor() {
        QueryProcessor qp = new QueryProcessor(lookup, true);
        qp.context = this.context;
        return qp;
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

    public void addGroupBy(String item) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupBy(item={})", item);
        }

        addGroupByClause(item);
        hasGrpBy = true;
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

        updatePosition(GremlinClause.LIMIT);
        add(GremlinClause.TO_LIST);
        updatePosition(GremlinClause.INLINE_TRANSFORM_CALL);
    }

    public String getText() {
        String ret;
        String[] items = new String[queryClauses.size()];

        int startIdx = hasSelect ? 1 : 0;
        int endIdx = hasSelect ? queryClauses.size() - 1 : queryClauses.size();
        for (int i = startIdx; i < endIdx; i++) {
            items[i] = queryClauses.getValue(i);
        }

        if (hasSelect) {
            String body = StringUtils.join(Stream.of(items).filter(Objects::nonNull).toArray(), ".");
            String inlineFn = queryClauses.getValue(queryClauses.size() - 1);
            String funCall = String.format(inlineFn, body);
            ret = queryClauses.getValue(0) + funCall;
        } else {
            ret = String.join(".", items);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("getText() => {}", ret);
        }
        return ret;
    }

    public boolean hasSelect() {
        return hasSelect;
    }

    public void addOrderBy(String name, boolean isDesc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderBy(name={}, isDesc={})", name, isDesc);
        }

        addOrderByClause(name, isDesc);
    }

    private void updatePosition(GremlinClause clause) {
        int index = queryClauses.hasClause(clause);
        if(-1 == index) {
            return;
        }

        GremlinClauseValue gcv = queryClauses.remove(index);
        queryClauses.add(gcv);
    }

    private void init() {
        if (!isNestedQuery) {
            add(GremlinClause.G);
            add(GremlinClause.V);
        } else {
            add(GremlinClause.NESTED_START);
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

    private String getJoinedQuotedStr(String[] elements) {
        StringJoiner joiner = new StringJoiner(",");
        Arrays.stream(elements).map(x -> "'" + x + "'").forEach(joiner::add);
        return joiner.toString();
    }

    private void add(GremlinClause clause, String... args) {
        queryClauses.add(new GremlinClauseValue(clause, clause.get(args)));
    }

    private void add(int idx, GremlinClause clause, String... args) {
        queryClauses.add(idx, new GremlinClauseValue(clause, clause.get(args)));
    }

    private void addRangeClause(String startIndex, String endIndex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addRangeClause(startIndex={}, endIndex={})", startIndex, endIndex);
        }

        if (hasSelect) {
            add(queryClauses.size() - 1, GremlinClause.RANGE, startIndex, startIndex, endIndex);
        } else {
            add(GremlinClause.RANGE, startIndex, startIndex, endIndex);
        }
    }

    private void addOrderByClause(String name, boolean descr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderByClause(name={})", name, descr);
        }

        IdentifierHelper.Advice ia = getAdvice(name);
        add((!descr) ? GremlinClause.ORDER_BY : GremlinClause.ORDER_BY_DESC, ia.getQualifiedName());
    }

    private void addGroupByClause(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupByClause(name={})", name);
        }

        IdentifierHelper.Advice ia = getAdvice(name);
        add(GremlinClause.GROUP_BY, ia.getQualifiedName());
    }

    private enum GremlinClause {
        AS("as('%s')"),
        DEDUP("dedup()"),
        G("g"),
        GROUP_BY("group().by('%')"),
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
        ORDER_BY("order().by('%s')"),
        ORDER_BY_DESC("order().by('%s', decr)"),
        OUT("out('%s')"),
        RANGE("range(%s, %s + %s)"),
        SELECT("select('%s')"),
        TO_LIST("toList()"),
        TEXT_CONTAINS("has('%s', org.janusgraph.core.attribute.Text.textRegex(%s))"),
        TEXT_PREFIX("has('%s', org.janusgraph.core.attribute.Text.textPrefix(%s))"),
        TEXT_SUFFIX("has('%s', org.janusgraph.core.attribute.Text.textRegex(\".*\" + %s))"),
        TRAIT("has('__traitNames', within('%s'))"),
        SELECT_EXPR_NOOP_FN("def f(r){ r }; "),
        SELECT_EXPR_HELPER_FN("def f(r){ return [[%s]].plus(r.collect({[%s]})).unique(); }; "),
        SELECT_WITH_GRPBY_HELPER_FN("def f(r){ return [[%s]].plus(r.collect({it.values()}).flatten().collect({[%s]})).unique(); }; "),
        INLINE_COUNT("r.size()"),
        INLINE_SUM("r.sum({it.value('%s')}).value('%s')"),
        INLINE_MAX("r.max({it.value('%s')}).value('%s')"),
        INLINE_MIN("r.min({it.value('%s')}).value('%s')"),
        INLINE_GET_PROPERTY("it.value('%s')"),
        INLINE_OUT_VERTEX("it.out('%s')"),
        INLINE_OUT_VERTEX_VALUE("it.out('%s').value('%s')"), // This might require more closure introduction :(
        INLINE_TRANSFORM_CALL("f(%s)"),
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

        public void add(int idx, GremlinClauseValue g) {
            list.add(idx, g);
        }

        public void add(GremlinClauseValue g, AtlasEntityType t) {
            add(g);
        }

        public void add(int idx, GremlinClauseValue g, AtlasEntityType t) {
            add(idx, g);
        }

        public void add(GremlinClause clause, String... args) {
            list.add(new GremlinClauseValue(clause, clause.get(args)));
        }

        public void add(int i, GremlinClause clause, String... args) {
            list.add(i, new GremlinClauseValue(clause, clause.get(args)));
        }

        public GremlinClauseValue getAt(int i) {
            return list.get(i);
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
        Map<String, String> aliasMap = new HashMap<>();
        private AtlasType activeType;

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
            if (et == null) {
                return false;
            }

            AtlasType attr = et.getAttributeType(attributeName);
            return attr != null && attr.getTypeName().equals(AtlasBaseTypeDef.ATLAS_TYPE_DATE);

        }
    }

    static class SelectExprMetadata {
        private String[] items;
        private String[] labels;

        private int countIdx = -1;
        private int sumIdx   = -1;
        private int maxIdx   = -1;
        private int minIdx   = -1;

        public String[] getItems() {
            return items;
        }

        public int getCountIdx() {
            return countIdx;
        }

        public void setCountIdx(final int countIdx) {
            this.countIdx = countIdx;
        }

        public int getSumIdx() {
            return sumIdx;
        }

        public void setSumIdx(final int sumIdx) {
            this.sumIdx = sumIdx;
        }

        public int getMaxIdx() {
            return maxIdx;
        }

        public void setMaxIdx(final int maxIdx) {
            this.maxIdx = maxIdx;
        }

        public int getMinIdx() {
            return minIdx;
        }

        public void setMinIdx(final int minIdx) {
            this.minIdx = minIdx;
        }

        public String[] getLabels() {
            return labels;
        }

        public void setItems(final String[] items) {
            this.items = items;
        }

        public void setLabels(final String[] labels) {
            this.labels = labels;
        }
    }
}
