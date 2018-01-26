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
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GremlinQueryComposer {
    private static final Logger LOG = LoggerFactory.getLogger(GremlinQueryComposer.class);

    private final String EMPTY_STRING = "";
    private static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private final int DEFAULT_QUERY_RESULT_LIMIT = 25;
    private final int DEFAULT_QUERY_RESULT_OFFSET = 0;

    private final GremlinClauseList      queryClauses   = new GremlinClauseList();
    private final Lookup                 lookup;
    private final boolean                isNestedQuery;
    private final AtlasDSL.QueryMetadata queryMetadata;
    private       int                    providedLimit  = DEFAULT_QUERY_RESULT_LIMIT;
    private       int                    providedOffset = DEFAULT_QUERY_RESULT_OFFSET;
    private       Context                context;

    private static final ThreadLocal<DateFormat> DSL_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        DateFormat ret = new SimpleDateFormat(ISO8601_FORMAT);
        ret.setTimeZone(TimeZone.getTimeZone("UTC"));
        return ret;
    });

    public GremlinQueryComposer(Lookup registryLookup, final AtlasDSL.QueryMetadata qmd, boolean isNestedQuery) {
        this.isNestedQuery = isNestedQuery;
        this.lookup        = registryLookup;
        this.queryMetadata = qmd;

        init();
    }
    public GremlinQueryComposer(AtlasTypeRegistry typeRegistry, final AtlasDSL.QueryMetadata qmd, int limit, int offset) {
        this(new RegistryBasedLookup(typeRegistry), qmd, false);
        this.context  = new Context(lookup);

        providedLimit = limit;
        providedOffset = offset < 0 ? DEFAULT_QUERY_RESULT_OFFSET : offset;
    }

    @VisibleForTesting
    GremlinQueryComposer(Lookup lookup, Context context, final AtlasDSL.QueryMetadata qmd) {
        this.isNestedQuery = false;
        this.lookup        = lookup;
        this.context       = context;
        this.queryMetadata = qmd;

        init();
    }

    public void addFrom(String typeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFrom(typeName={})", typeName);
        }

        IdentifierHelper.IdentifierMetadata ta = getIdMetadata(typeName);

        if(context.shouldRegister(ta.get())) {
            context.registerActive(ta.get());

            IdentifierHelper.IdentifierMetadata ia = getIdMetadata(ta.get());

            if (ia.isTrait()) {
                add(GremlinClause.TRAIT, ia);
            } else {
                if (ia.hasSubtypes()) {
                    add(GremlinClause.HAS_TYPE_WITHIN, ia.getSubTypes());
                } else {
                    add(GremlinClause.HAS_TYPE, ia);
                }
            }
        } else {
            IdentifierHelper.IdentifierMetadata ia = getIdMetadata(ta.get());
            introduceType(ia);
        }
    }

    public void addFromProperty(String typeName, String attribute) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromProperty(typeName={}, attribute={})", typeName, attribute);
        }

        if(!isNestedQuery) {
            addFrom(typeName);
        }

        add(GremlinClause.HAS_PROPERTY, getIdMetadata(attribute));
    }

    public void addIsA(String typeName, String traitName) {
        if (!isNestedQuery) {
            addFrom(typeName);
        }

        IdentifierHelper.IdentifierMetadata ia = getIdMetadata(traitName);
        add(GremlinClause.TRAIT, ia);
    }

    public void addWhere(String lhs, String operator, String rhs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addWhere(lhs={}, operator={}, rhs={})", lhs, operator, rhs);
        }

        String                              currentType = context.getActiveTypeName();
        IdentifierHelper.IdentifierMetadata org         = null;
        IdentifierHelper.IdentifierMetadata lhsI        = getIdMetadata(lhs);
        if (!lhsI.isPrimitive()) {
            introduceType(lhsI);
            org = lhsI;
            lhsI = getIdMetadata(lhs);
        }

        if (!context.validator.isValidQualifiedName(lhsI.getQualifiedName(), lhsI.getRaw())) {
            return;
        }

        if (lhsI.isDate()) {
            rhs = parseDate(rhs);
        }

        SearchParameters.Operator op = SearchParameters.Operator.fromString(operator);
        rhs = addQuotesIfNecessary(rhs);
        if (op == SearchParameters.Operator.LIKE) {
            add(GremlinClause.TEXT_CONTAINS, lhsI.getQualifiedName(), IdentifierHelper.getFixedRegEx(rhs));
        } else if (op == SearchParameters.Operator.IN) {
            add(GremlinClause.HAS_OPERATOR, lhsI.getQualifiedName(), "within", rhs);
        } else {
            add(GremlinClause.HAS_OPERATOR, lhsI.getQualifiedName(), op.getSymbols()[1], rhs);
        }

        if (org != null && org.isReferredType()) {
            add(GremlinClause.DEDUP);
            add(GremlinClause.IN, org.getEdgeLabel());
            context.registerActive(currentType);
        }
    }

    private String getQualifiedName(IdentifierHelper.IdentifierMetadata ia) {
        return context.validator.isValidQualifiedName(ia.getQualifiedName(), ia.getRaw()) ?
                ia.getQualifiedName() : ia.getRaw();
    }

    public void addAndClauses(List<String> clauses) {
        add(GremlinClause.AND, String.join(",", clauses));
    }

    public void addOrClauses(List<String> clauses) {
        add(GremlinClause.OR, String.join(",", clauses));
    }

    public void addSelect(SelectClauseComposer selectClauseComposer) {
        process(selectClauseComposer);
        if (!(queryMetadata.hasOrderBy() && queryMetadata.hasGroupBy())) {
            addSelectTransformation(selectClauseComposer, null, false);
        }
        this.context.setSelectClauseComposer(selectClauseComposer);
    }

    private void process(SelectClauseComposer scc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addSelect(items.length={})", scc.getItems() != null ? scc.getItems().length : 0);
        }

        if (scc.getItems() == null) {
            return;
        }

        for (int i = 0; i < scc.getItems().length; i++) {
            IdentifierHelper.IdentifierMetadata ia = getIdMetadata(scc.getItem(i));

            if(scc.isAggregatorWithArgument(i) && !ia.isPrimitive()) {
                context.check(false, AtlasErrorCode.INVALID_DSL_SELECT_INVALID_AGG, ia.getQualifiedName());
                return;
            }

            if (!scc.getItem(i).equals(scc.getLabel(i))) {
                context.addAlias(scc.getLabel(i), getQualifiedName(ia));
            }

            if (scc.updateAsApplicable(i, getQualifiedName(ia))) {
                continue;
            }

            scc.isSelectNoop = hasNoopCondition(ia);
            if (scc.isSelectNoop) {
                return;
            }

            if (introduceType(ia)) {
                scc.incrementTypesIntroduced();
                scc.isSelectNoop = !ia.hasParts();
                if (ia.hasParts()) {
                    scc.assign(i, getQualifiedName(getIdMetadata(ia.get())), GremlinClause.INLINE_GET_PROPERTY);
                }
            } else {
                scc.assign(i, getQualifiedName(ia), GremlinClause.INLINE_GET_PROPERTY);
                scc.incrementPrimitiveType();
            }
        }

        context.validator.check(!scc.hasMultipleReferredTypes(),
                AtlasErrorCode.INVALID_DSL_SELECT_REFERRED_ATTR, Integer.toString(scc.getIntroducedTypesCount()));
        context.validator.check(!scc.hasMixedAttributes(), AtlasErrorCode.INVALID_DSL_SELECT_ATTR_MIXING);
    }

    private boolean hasNoopCondition(IdentifierHelper.IdentifierMetadata ia) {
        return !ia.isPrimitive() && !ia.isAttribute() && context.hasAlias(ia.getRaw());
    }

    public GremlinQueryComposer createNestedProcessor() {
        GremlinQueryComposer qp = new GremlinQueryComposer(lookup, queryMetadata, true);
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

    public void addAsClause(String alias) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addAsClause(stepName={})", alias);
        }

        add(GremlinClause.AS, alias);
    }

    public void addGroupBy(String item) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupBy(item={})", item);
        }

        addGroupByClause(item);
    }

    public void addLimit(String limit, String offset) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addLimit(limit={}, offset={})", limit, offset);
        }

        if (offset.equalsIgnoreCase("0")) {
            add(GremlinClause.LIMIT, limit, limit);
        } else {
            addRangeClause(offset, limit);
        }
    }

    public void addDefaultLimit() {
        addLimit(Integer.toString(providedLimit), Integer.toString(providedOffset));
    }

    public String get() {
        close();

        String items[] = getFormattedClauses(queryMetadata.needTransformation());
        String s = queryMetadata.needTransformation() ?
                getTransformedClauses(items) :
                String.join(".", items);

        if(LOG.isDebugEnabled()) {
            LOG.debug("Gremlin: {}", s);
        }

        return s;
    }

    public List<String> getErrorList() {
        return context.getErrorList();
    }

    private String getTransformedClauses(String[] items) {
        String ret;
        String body = String.join(".", Stream.of(items).filter(Objects::nonNull).collect(Collectors.toList()));
        String inlineFn = queryClauses.getValue(queryClauses.size() - 1);
        String funCall = String.format(inlineFn, body);
        if (isNestedQuery) {
            ret = String.join(".", queryClauses.getValue(0), funCall);
        } else {
            ret = queryClauses.getValue(0) + funCall;
        }
        return ret;
    }

    private String[] getFormattedClauses(boolean needTransformation) {
        String[] items = new String[queryClauses.size()];
        int startIdx = needTransformation ? 1 : 0;
        int endIdx = needTransformation ? queryClauses.size() - 1 : queryClauses.size();

        for (int i = startIdx; i < endIdx; i++) {
            items[i] = queryClauses.getValue(i);
        }
        return items;
    }

    public void addOrderBy(String name, boolean isDesc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderBy(name={}, isDesc={})", name, isDesc);
        }

        IdentifierHelper.IdentifierMetadata ia = getIdMetadata(name);
        if (queryMetadata.hasSelect() && queryMetadata.hasGroupBy()) {
            addSelectTransformation(this.context.selectClauseComposer, getQualifiedName(ia), isDesc);
        } else if (queryMetadata.hasGroupBy()) {
            addOrderByClause(getQualifiedName(ia), isDesc);
            moveToLast(GremlinClause.GROUP_BY);
        } else {
            addOrderByClause(getQualifiedName(ia), isDesc);
        }
    }

    private void addSelectTransformation(final SelectClauseComposer selectClauseComposer,
                                         final String orderByQualifiedAttrName,
                                         final boolean isDesc) {
        GremlinClause fn;
        if (selectClauseComposer.isSelectNoop) {
            fn = GremlinClause.SELECT_NOOP_FN;
        } else if (queryMetadata.hasGroupBy()){
            fn = selectClauseComposer.onlyAggregators() ?
                    GremlinClause.SELECT_ONLY_AGG_GRP_FN :
                         GremlinClause.SELECT_MULTI_ATTR_GRP_FN;

        } else {
            fn = selectClauseComposer.onlyAggregators() ?
                    GremlinClause.SELECT_ONLY_AGG_FN :
                         GremlinClause.SELECT_FN;
        }
        if (StringUtils.isEmpty(orderByQualifiedAttrName)) {
            add(0, fn,
                             selectClauseComposer.getLabelHeader(),
                             selectClauseComposer.hasAssignmentExpr() ? selectClauseComposer.getAssignmentExprString(): EMPTY_STRING,
                             selectClauseComposer.getItemsString(), EMPTY_STRING);
        } else {
            int itemIdx = selectClauseComposer.getAttrIndex(orderByQualifiedAttrName);
            GremlinClause sortClause = GremlinClause.INLINE_DEFAULT_SORT;
            if (itemIdx != -1) {
                sortClause = isDesc ? GremlinClause.INLINE_SORT_DESC : GremlinClause.INLINE_SORT_ASC;
            }
            String idxStr = String.valueOf(itemIdx);
            add(0, fn,
                             selectClauseComposer.getLabelHeader(),
                             selectClauseComposer.hasAssignmentExpr() ? selectClauseComposer.getAssignmentExprString(): EMPTY_STRING,
                             selectClauseComposer.getItemsString(),
                             sortClause.get(idxStr, idxStr)
                             );
        }

        add(GremlinClause.INLINE_TRANSFORM_CALL);
    }

    private String addQuotesIfNecessary(String rhs) {
        if(IdentifierHelper.isTrueOrFalse(rhs)) return rhs;
        if(IdentifierHelper.isQuoted(rhs)) return rhs;
        return IdentifierHelper.getQuoted(rhs);
    }

    private String parseDate(String rhs) {
        String s = IdentifierHelper.isQuoted(rhs) ?
                IdentifierHelper.removeQuotes(rhs) :
                rhs;


        return String.format("'%d'", getDateFormat(s));
    }

    public long getDateFormat(String s) {
        try {
            return DSL_DATE_FORMAT.get().parse(s).getTime();
        } catch (ParseException ex) {
            context.validator.check(ex, AtlasErrorCode.INVALID_DSL_INVALID_DATE);
        }

        return -1;
    }

    private void close() {
        if (isNestedQuery)
            return;

        if (!queryMetadata.hasLimitOffset()) {
            addDefaultLimit();
        }

        if (queryClauses.isEmpty()) {
            queryClauses.clear();
            return;
        }

        moveToLast(GremlinClause.LIMIT);
        add(GremlinClause.TO_LIST);
        moveToLast(GremlinClause.INLINE_TRANSFORM_CALL);
    }

    private void moveToLast(GremlinClause clause) {
        int index = queryClauses.contains(clause);
        if (-1 == index) {
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

    private boolean introduceType(IdentifierHelper.IdentifierMetadata ia) {
        if (ia.isReferredType()) {
            add(GremlinClause.OUT, ia.getEdgeLabel());
            context.registerActive(ia);
        }

        return ia.isReferredType();
    }

    private IdentifierHelper.IdentifierMetadata getIdMetadata(String actualTypeName) {
        return IdentifierHelper.create(context, lookup, actualTypeName);
    }

    private void addRangeClause(String startIndex, String endIndex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addRangeClause(startIndex={}, endIndex={})", startIndex, endIndex);
        }

        if (queryMetadata.hasSelect()) {
            add(queryClauses.size() - 1, GremlinClause.RANGE, startIndex, startIndex, endIndex, startIndex, startIndex, endIndex);
        } else {
            add(GremlinClause.RANGE, startIndex, startIndex, endIndex, startIndex, startIndex, endIndex);
        }
    }

    private void addOrderByClause(String name, boolean descr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderByClause(name={})", name, descr);
        }

        IdentifierHelper.IdentifierMetadata ia = getIdMetadata(name);
        add((!descr) ? GremlinClause.ORDER_BY : GremlinClause.ORDER_BY_DESC, ia);
    }

    private void addGroupByClause(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupByClause(name={})", name);
        }

        IdentifierHelper.IdentifierMetadata ia = getIdMetadata(name);
        add(GremlinClause.GROUP_BY, ia);
    }

    public boolean hasFromClause() {
        return queryClauses.contains(GremlinClause.HAS_TYPE) != -1 ||
                queryClauses.contains(GremlinClause.HAS_TYPE_WITHIN) != -1;
    }

    private void add(GremlinClause clause, IdentifierHelper.IdentifierMetadata ia) {
        if(context != null && !context.validator.isValid(context, clause, ia)) {
            return;
        }

        add(clause, (ia.getQualifiedName() == null ? ia.get() : ia.getQualifiedName()));
    }

    private void add(GremlinClause clause, String... args) {
        queryClauses.add(new GremlinClauseValue(clause, clause.get(args)));
    }

    private void add(int idx, GremlinClause clause, String... args) {
        queryClauses.add(idx, new GremlinClauseValue(clause, clause.get(args)));
    }

    static class GremlinClauseValue {
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

    @VisibleForTesting
    static class Context {
        private static final AtlasStructType UNKNOWN_TYPE = new AtlasStructType(new AtlasStructDef());

        private final Lookup               lookup;
        private final Map<String, String>  aliasMap = new HashMap<>();
        private       AtlasType            activeType;
        private       SelectClauseComposer selectClauseComposer;
        private       ClauseValidator        validator;

        public Context(Lookup lookup) {
            this.lookup = lookup;
            validator = new ClauseValidator(lookup);
        }

        public void registerActive(String typeName) {
            if(shouldRegister(typeName)) {
                try {
                    activeType = lookup.getType(typeName);
                    aliasMap.put(typeName, typeName);
                } catch (AtlasBaseException e) {
                    validator.check(e, AtlasErrorCode.INVALID_DSL_UNKNOWN_TYPE, typeName);
                    activeType = UNKNOWN_TYPE;
                }
            }
        }

        public void registerActive(IdentifierHelper.IdentifierMetadata identifierMetadata) {
            if (validator.check(StringUtils.isNotEmpty(identifierMetadata.getTypeName()),
                                AtlasErrorCode.INVALID_DSL_UNKNOWN_TYPE, identifierMetadata.getRaw())) {
                registerActive(identifierMetadata.getTypeName());
            } else {
                activeType = UNKNOWN_TYPE;
            }
        }

        public AtlasEntityType getActiveEntityType() {
            return (activeType instanceof AtlasEntityType) ?
                    (AtlasEntityType) activeType :
                    null;
        }

        public String getActiveTypeName() {
            return activeType.getTypeName();
        }

        public AtlasType getActiveType() {
            return activeType;
        }

        public boolean shouldRegister(String typeName) {
            return activeType == null ||
                    (activeType != null && !StringUtils.equals(getActiveTypeName(), typeName)) &&
                            (activeType != null && !lookup.hasAttribute(this, typeName));
        }

        public void registerAlias(String alias) {
            addAlias(alias, getActiveTypeName());
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

        public void setSelectClauseComposer(SelectClauseComposer selectClauseComposer) {
            this.selectClauseComposer = selectClauseComposer;
        }

        public void addAlias(String alias, String typeName) {
            if(aliasMap.containsKey(alias)) {
                check(false, AtlasErrorCode.INVALID_DSL_DUPLICATE_ALIAS, alias, getActiveTypeName());
                return;
            }

            aliasMap.put(alias, typeName);
        }

        public List<String> getErrorList() {
            return validator.getErrorList();
        }

        public boolean error(AtlasBaseException e, AtlasErrorCode ec, String t, String name) {
            return validator.check(e, ec, t, name);
        }

        public boolean check(boolean condition, AtlasErrorCode vm, String... args) {
            return validator.check(condition, vm, args);
        }
    }

    private static class ClauseValidator {
        private final Lookup lookup;
        List<String> errorList = new ArrayList<>();

        public ClauseValidator(Lookup lookup) {
            this.lookup = lookup;
        }

        public boolean isValid(Context ctx, GremlinClause clause, IdentifierHelper.IdentifierMetadata ia) {
            switch (clause) {
                case TRAIT:
                    return check(ia.isTrait(), AtlasErrorCode.INVALID_DSL_UNKNOWN_CLASSIFICATION, ia.getRaw());

                case HAS_TYPE:
                    TypeCategory typeCategory = ctx.getActiveType().getTypeCategory();
                    return check(StringUtils.isNotEmpty(ia.getTypeName()) &&
                            typeCategory == TypeCategory.CLASSIFICATION || typeCategory == TypeCategory.ENTITY,
                                 AtlasErrorCode.INVALID_DSL_UNKNOWN_TYPE, ia.getRaw());

                case HAS_PROPERTY:
                    return check(ia.isPrimitive(), AtlasErrorCode.INVALID_DSL_HAS_PROPERTY, ia.getRaw());

                case ORDER_BY:
                    return check(ia.isPrimitive(), AtlasErrorCode.INVALID_DSL_ORDERBY, ia.getRaw());

                case GROUP_BY:
                    return check(ia.isPrimitive(), AtlasErrorCode.INVALID_DSL_SELECT_INVALID_AGG, ia.getRaw());

                default:
                    return (getErrorList().size() == 0);
            }
        }

        public boolean check(Exception ex, AtlasErrorCode vm, String... args) {
            String[] extraArgs = getExtraSlotArgs(args, ex.getMessage());
            return check(false, vm, extraArgs);
        }

        private String[] getExtraSlotArgs(String[] args, String s) {
            String[] argsPlus1 = new String[args.length + 1];
            System.arraycopy(args, 0, argsPlus1, 0, args.length);
            argsPlus1[args.length] = s;
            return argsPlus1;
        }

        public boolean check(boolean condition, AtlasErrorCode vm, String... args) {
            if(!condition) {
                addError(vm, args);
            }

            return condition;
        }

        public void addError(AtlasErrorCode ec, String... messages) {
            errorList.add(ec.getFormattedErrorMessage(messages));
        }

        public List<String> getErrorList() {
            return errorList;
        }

        public boolean isValidQualifiedName(String qualifiedName, String raw) {
            return check(StringUtils.isNotEmpty(qualifiedName), AtlasErrorCode.INVALID_DSL_QUALIFIED_NAME, raw);
        }
    }
}
