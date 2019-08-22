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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.atlas.model.discovery.SearchParameters.ALL_CLASSIFICATIONS;
import static org.apache.atlas.model.discovery.SearchParameters.NO_CLASSIFICATIONS;

public class GremlinQueryComposer {
    private static final Logger LOG                 = LoggerFactory.getLogger(GremlinQueryComposer.class);
    private static final String ISO8601_FORMAT      = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd";

    private static final ThreadLocal<DateFormat[]> DSL_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        final String formats[] = {ISO8601_FORMAT, ISO8601_DATE_FORMAT};
        DateFormat[] dfs       = new DateFormat[formats.length];
        for (int i = 0; i < formats.length; i++) {
            dfs[i] = new SimpleDateFormat(formats[i]);
            dfs[i].setTimeZone(TimeZone.getTimeZone("UTC"));
        }
        return dfs;
    });

    private final String            EMPTY_STRING                = "";
    private final int               DEFAULT_QUERY_RESULT_LIMIT  = 25;
    private final int               DEFAULT_QUERY_RESULT_OFFSET = 0;
    private final GremlinClauseList queryClauses                = new GremlinClauseList();
    private final Set<String>       attributesProcessed         = new HashSet<>();
    private final Lookup                 lookup;
    private final boolean                isNestedQuery;
    private final AtlasDSL.QueryMetadata queryMetadata;
    private int providedLimit  = DEFAULT_QUERY_RESULT_LIMIT;
    private int providedOffset = DEFAULT_QUERY_RESULT_OFFSET;
    private Context context;

    public GremlinQueryComposer(Lookup registryLookup, final AtlasDSL.QueryMetadata qmd, boolean isNestedQuery) {
        this.isNestedQuery = isNestedQuery;
        this.lookup = registryLookup;
        this.queryMetadata = qmd;

        init();
    }

    public GremlinQueryComposer(AtlasTypeRegistry typeRegistry, final AtlasDSL.QueryMetadata qmd, int limit, int offset) {
        this(new RegistryBasedLookup(typeRegistry), qmd, false);
        this.context = new Context(lookup);

        providedLimit = limit;
        providedOffset = offset < 0 ? DEFAULT_QUERY_RESULT_OFFSET : offset;
    }

    @VisibleForTesting
    GremlinQueryComposer(Lookup lookup, Context context, final AtlasDSL.QueryMetadata qmd) {
        this.isNestedQuery = false;
        this.lookup = lookup;
        this.context = context;
        this.queryMetadata = qmd;

        init();
    }

    public void addFrom(String typeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFrom(typeName={})", typeName);
        }

        IdentifierHelper.Info typeInfo = createInfo(typeName);

        if (context.shouldRegister(typeInfo.get())) {
            context.registerActive(typeInfo.get());

            IdentifierHelper.Info ia = createInfo(typeInfo.get());

            if (ia.isTrait()) {
                String traitName = ia.get();

                if (traitName.equalsIgnoreCase(ALL_CLASSIFICATIONS)) {
                    addTrait(GremlinClause.ANY_TRAIT, ia);
                } else if (traitName.equalsIgnoreCase(NO_CLASSIFICATIONS)) {
                    addTrait(GremlinClause.NO_TRAIT, ia);
                } else {
                    addTrait(GremlinClause.TRAIT, ia);
                }
            } else {
                if (ia.hasSubtypes()) {
                    add(GremlinClause.HAS_TYPE_WITHIN, ia.getSubTypes());
                } else {
                    add(GremlinClause.HAS_TYPE, ia);
                }
            }
        } else {
            IdentifierHelper.Info ia = createInfo(typeInfo.get());
            introduceType(ia);
        }
    }

    public void addFromProperty(String typeName, String attribute) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromProperty(typeName={}, attribute={})", typeName, attribute);
        }

        if (!isNestedQuery) {
            addFrom(typeName);
        }

        add(GremlinClause.HAS_PROPERTY, createInfo(attribute));
    }

    public void addIsA(String typeName, String traitName) {
        if (!isNestedQuery) {
            addFrom(typeName);
        }

        IdentifierHelper.Info traitInfo = createInfo(traitName);

        if (StringUtils.equals(traitName, ALL_CLASSIFICATIONS)) {
            addTrait(GremlinClause.ANY_TRAIT, traitInfo);
        } else if (StringUtils.equals(traitName, NO_CLASSIFICATIONS)) {
            addTrait(GremlinClause.NO_TRAIT, traitInfo);
        } else {
            addTrait(GremlinClause.TRAIT, traitInfo);
        }
    }

    public void addWhere(String lhs, String operator, String rhs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addWhere(lhs={}, operator={}, rhs={})", lhs, operator, rhs);
        }

        String                currentType = context.getActiveTypeName();

        IdentifierHelper.Info org         = null;
        IdentifierHelper.Info lhsI        = createInfo(lhs);
        if (!lhsI.isPrimitive()) {
            introduceType(lhsI);
            org = lhsI;
            lhsI = createInfo(lhs);
            lhsI.setTypeName(org.getTypeName());
        }

        if (!context.validator.isValidQualifiedName(lhsI.getQualifiedName(), lhsI.getRaw())) {
            return;
        }

        if (lhsI.isDate()) {
            rhs = parseDate(rhs);
        } else if (lhsI.isNumeric()) {
            rhs = parseNumber(rhs, this.context);
        }

        rhs = addQuotesIfNecessary(lhsI, rhs);
        SearchParameters.Operator op = SearchParameters.Operator.fromString(operator);
        if (op == SearchParameters.Operator.LIKE) {
            final AtlasStructType.AtlasAttribute attribute = context.getActiveEntityType().getAttribute(lhsI.getAttributeName());
            final AtlasStructDef.AtlasAttributeDef.IndexType indexType = attribute.getAttributeDef().getIndexType();

            if (indexType == AtlasStructDef.AtlasAttributeDef.IndexType.STRING) {
                add(GremlinClause.STRING_CONTAINS, getPropertyForClause(lhsI), IdentifierHelper.getFixedRegEx(rhs));
            } else {
                add(GremlinClause.TEXT_CONTAINS, getPropertyForClause(lhsI), IdentifierHelper.getFixedRegEx(rhs));
            }
        } else if (op == SearchParameters.Operator.IN) {
            add(GremlinClause.HAS_OPERATOR, getPropertyForClause(lhsI), "within", rhs);
        } else {
            add(GremlinClause.HAS_OPERATOR, getPropertyForClause(lhsI), op.getSymbols()[1], rhs);
        }
        // record that the attribute has been processed so that the select clause doesn't add a attr presence check
        attributesProcessed.add(lhsI.getQualifiedName());

        if (org != null && org.isReferredType()) {
            add(GremlinClause.DEDUP);
            add(GremlinClause.IN, org.getEdgeLabel());
            context.registerActive(currentType);
        }
    }

    private String parseNumber(String rhs, Context context) {
        return rhs.replace("'", "").replace("\"", "") + context.getNumericTypeFormatter();
    }

    public void addAndClauses(List<String> clauses) {
        add(GremlinClause.AND, String.join(",", clauses));
    }

    public void addOrClauses(List<String> clauses) {
        add(GremlinClause.OR, String.join(",", clauses));
    }

    public Set<String> getAttributesProcessed() {
        return attributesProcessed;
    }

    public void addProcessedAttributes(Set<String> attributesProcessed) {
        this.attributesProcessed.addAll(attributesProcessed);
    }

    public void addSelect(SelectClauseComposer selectClauseComposer) {
        process(selectClauseComposer);

        if (CollectionUtils.isEmpty(context.getErrorList())) {
            addSelectAttrExistsCheck(selectClauseComposer);
        }

        // If the query contains orderBy and groupBy then the transformation determination is deferred to the method processing orderBy
        if (!(queryMetadata.hasOrderBy() && queryMetadata.hasGroupBy())) {
            addSelectTransformation(selectClauseComposer, null, false);
        }
        this.context.setSelectClauseComposer(selectClauseComposer);
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

        SelectClauseComposer scc = context.getSelectClauseComposer();
        if (scc == null) {
            addLimitHelper(limit, offset);
        } else {
            if (!scc.hasAggregators()) {
                addLimitHelper(limit, offset);
            }
        }
    }

    public void addDefaultLimit() {
        addLimit(Integer.toString(providedLimit), Integer.toString(providedOffset));
    }

    public String get() {
        close();

        boolean mustTransform = !isNestedQuery && queryMetadata.needTransformation();
        String  items[]       = getFormattedClauses(mustTransform);
        String s = mustTransform ?
                           getTransformedClauses(items) :
                           String.join(".", items);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Gremlin: {}", s);
        }

        return s;
    }

    public List<String> getErrorList() {
        return context.getErrorList();
    }

    public void addOrderBy(String name, boolean isDesc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderBy(name={}, isDesc={})", name, isDesc);
        }

        IdentifierHelper.Info ia = createInfo(name);
        if (queryMetadata.hasSelect() && queryMetadata.hasGroupBy()) {
            addSelectTransformation(this.context.selectClauseComposer, getPropertyForClause(ia), isDesc);
        } else if (queryMetadata.hasGroupBy()) {
            addOrderByClause(ia, isDesc);
            moveToLast(GremlinClause.GROUP_BY);
        } else {
            addOrderByClause(ia, isDesc);
        }
    }

    public long getDateFormat(String s) {

        for (DateFormat dateFormat : DSL_DATE_FORMAT.get()) {
            try {
                return dateFormat.parse(s).getTime();
            } catch (ParseException ignored) {
            }
        }

        context.validator.check(false, AtlasErrorCode.INVALID_DSL_INVALID_DATE, s);
        return -1;
    }

    public boolean hasFromClause() {
        return queryClauses.contains(GremlinClause.HAS_TYPE) != -1 ||
                       queryClauses.contains(GremlinClause.HAS_TYPE_WITHIN) != -1;
    }

    private String getPropertyForClause(IdentifierHelper.Info ia) {
        String vertexPropertyName = lookup.getVertexPropertyName(ia.getTypeName(), ia.getAttributeName());
        if (StringUtils.isNotEmpty(vertexPropertyName)) {
            return vertexPropertyName;
        }

        if (StringUtils.isNotEmpty(ia.getQualifiedName())) {
            return ia.getQualifiedName();
        }

        return ia.getRaw();
    }

    private void addSelectAttrExistsCheck(final SelectClauseComposer selectClauseComposer) {
        // For each of the select attributes we need to add a presence check as well, if there's no explicit where for the same
        // NOTE: One side-effect is that the result table will be empty if any of the attributes is null or empty for the type
        String[] qualifiedAttributes = selectClauseComposer.getAttributes();
        if (qualifiedAttributes != null && qualifiedAttributes.length > 0) {
            for (int i = 0; i < qualifiedAttributes.length; i++) {
                String                qualifiedAttribute = qualifiedAttributes[i];
                IdentifierHelper.Info idMetadata         = createInfo(qualifiedAttribute);
                // Only primitive attributes need to be checked
                if (idMetadata.isPrimitive() && !selectClauseComposer.isAggregatorIdx(i) && !attributesProcessed.contains(qualifiedAttribute)) {
                    add(GremlinClause.HAS_PROPERTY, getPropertyForClause(idMetadata));
                }
            }
            // All these checks should be done before the grouping happens (if any)
            moveToLast(GremlinClause.GROUP_BY);
        }
    }

    private void process(SelectClauseComposer scc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addSelect(items.length={})", scc.getItems() != null ? scc.getItems().length : 0);
        }

        if (scc.getItems() == null) {
            return;
        }

        for (int i = 0; i < scc.getItems().length; i++) {
            IdentifierHelper.Info ia = createInfo(scc.getItem(i));

            if (scc.isAggregatorWithArgument(i) && !ia.isPrimitive()) {
                context.check(false, AtlasErrorCode.INVALID_DSL_SELECT_INVALID_AGG, ia.getQualifiedName());
                return;
            }

            if (!scc.getItem(i).equals(scc.getLabel(i))) {
                context.addAlias(scc.getLabel(i), ia.getQualifiedName());
            }

            if (scc.updateAsApplicable(i, getPropertyForClause(ia), ia.getQualifiedName())) {
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
                    scc.assign(i, getPropertyForClause(createInfo(ia.get())), GremlinClause.INLINE_GET_PROPERTY);
                }
            } else {
                scc.assign(i, getPropertyForClause(ia), GremlinClause.INLINE_GET_PROPERTY);
                scc.incrementPrimitiveType();
            }
        }

        context.validator.check(!scc.hasMultipleReferredTypes(),
                                AtlasErrorCode.INVALID_DSL_SELECT_REFERRED_ATTR, Integer.toString(scc.getIntroducedTypesCount()));
        context.validator.check(!scc.hasMixedAttributes(), AtlasErrorCode.INVALID_DSL_SELECT_ATTR_MIXING);
    }

    private boolean hasNoopCondition(IdentifierHelper.Info ia) {
        return !ia.isPrimitive() && !ia.isAttribute() && context.hasAlias(ia.getRaw());
    }

    private void addLimitHelper(final String limit, final String offset) {
        if (offset.equalsIgnoreCase("0")) {
            add(GremlinClause.LIMIT, limit, limit);
        } else {
            addRangeClause(offset, limit);
        }
    }

    private String getTransformedClauses(String[] items) {
        String ret;
        String body     = String.join(".", Stream.of(items).filter(Objects::nonNull).collect(Collectors.toList()));
        String inlineFn = queryClauses.getValue(queryClauses.size() - 1);
        String funCall  = String.format(inlineFn, body);
        if (isNestedQuery) {
            ret = String.join(".", queryClauses.getValue(0), funCall);
        } else {
            ret = queryClauses.getValue(0) + funCall;
        }
        return ret;
    }

    private String[] getFormattedClauses(boolean needTransformation) {
        String[] items    = new String[queryClauses.size()];
        int      startIdx = needTransformation ? 1 : 0;
        int      endIdx   = needTransformation ? queryClauses.size() - 1 : queryClauses.size();

        for (int i = startIdx; i < endIdx; i++) {
            items[i] = queryClauses.getValue(i);
        }
        return items;
    }

    private void addSelectTransformation(final SelectClauseComposer selectClauseComposer,
                                         final String orderByQualifiedAttrName,
                                         final boolean isDesc) {
        GremlinClause gremlinClause;
        if (selectClauseComposer.isSelectNoop) {
            gremlinClause = GremlinClause.SELECT_NOOP_FN;
        } else if (queryMetadata.hasGroupBy()) {
            gremlinClause = selectClauseComposer.onlyAggregators() ? GremlinClause.SELECT_ONLY_AGG_GRP_FN : GremlinClause.SELECT_MULTI_ATTR_GRP_FN;
        } else {
            gremlinClause = selectClauseComposer.onlyAggregators() ? GremlinClause.SELECT_ONLY_AGG_FN : GremlinClause.SELECT_FN;
        }
        if (StringUtils.isEmpty(orderByQualifiedAttrName)) {
            add(0, gremlinClause,
                selectClauseComposer.getLabelHeader(),
                selectClauseComposer.hasAssignmentExpr() ? selectClauseComposer.getAssignmentExprString() : EMPTY_STRING,
                selectClauseComposer.getItemsString(),
                EMPTY_STRING);
        } else {
            int           itemIdx    = selectClauseComposer.getAttrIndex(orderByQualifiedAttrName);
            GremlinClause sortClause = GremlinClause.INLINE_DEFAULT_TUPLE_SORT;
            if (itemIdx != -1) {
                sortClause = isDesc ? GremlinClause.INLINE_TUPLE_SORT_DESC : GremlinClause.INLINE_TUPLE_SORT_ASC;
            }
            String idxStr = String.valueOf(itemIdx);
            add(0, gremlinClause,
                selectClauseComposer.getLabelHeader(),
                selectClauseComposer.hasAssignmentExpr() ? selectClauseComposer.getAssignmentExprString() : EMPTY_STRING,
                selectClauseComposer.getItemsString(),
                sortClause.get(idxStr, idxStr)
            );
        }

        add(GremlinClause.INLINE_TRANSFORM_CALL);
    }

    private String addQuotesIfNecessary(IdentifierHelper.Info rhsI, String rhs) {
        if(rhsI.isNumeric()) return rhs;
        if (IdentifierHelper.isTrueOrFalse(rhs)) return rhs;
        if (IdentifierHelper.isQuoted(rhs)) return rhs;
        return IdentifierHelper.getQuoted(rhs);
    }

    private String parseDate(String rhs) {
        String s = IdentifierHelper.isQuoted(rhs) ?
                           IdentifierHelper.removeQuotes(rhs) :
                           rhs;


        return String.format("'%d'", getDateFormat(s));
    }

    private void close() {
        if (isNestedQuery)
            return;

        // Need de-duping at the end so that correct results are fetched
        if (queryClauses.size() > 2) {
            // QueryClauses should've something more that just g.V() (hence 2)
            add(GremlinClause.DEDUP);
            // Range and limit must be present after the de-duping construct
            moveToLast(GremlinClause.RANGE);
            moveToLast(GremlinClause.LIMIT);
        }

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

    private boolean introduceType(IdentifierHelper.Info ia) {
        if (ia.isReferredType()) {
            add(GremlinClause.OUT, ia.getEdgeLabel());
            context.registerActive(ia);
        }

        return ia.isReferredType();
    }

    private IdentifierHelper.Info createInfo(String actualTypeName) {
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

    private void addOrderByClause(IdentifierHelper.Info ia, boolean descr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderByClause(name={})", ia.getRaw(), descr);
        }

        add((!descr) ? GremlinClause.ORDER_BY : GremlinClause.ORDER_BY_DESC, ia);
    }

    private void addGroupByClause(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupByClause(name={})", name);
        }

        IdentifierHelper.Info ia = createInfo(name);
        add(GremlinClause.GROUP_BY, ia);
    }

    private void add(GremlinClause clause, IdentifierHelper.Info idInfo) {
        if (context != null && !context.validator.isValid(context, clause, idInfo)) {
            return;
        }

        add(clause, getPropertyForClause(idInfo));
    }

    private void add(GremlinClause clause, String... args) {
        queryClauses.add(new GremlinClauseValue(clause, clause.get(args)));
    }

    private void add(int idx, GremlinClause clause, String... args) {
        queryClauses.add(idx, new GremlinClauseValue(clause, clause.get(args)));
    }

    private void addTrait(GremlinClause clause, IdentifierHelper.Info idInfo) {
        if (context != null && !context.validator.isValid(context, clause, idInfo)) {
            return;
        }

        add(clause, idInfo.get(), idInfo.get());
    }

    static class GremlinClauseValue {
        private final GremlinClause clause;
        private final String        value;

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

        private final Lookup lookup;
        private final Map<String, String>   aliasMap = new HashMap<>();
        private AtlasType                   activeType;
        private SelectClauseComposer        selectClauseComposer;
        private ClauseValidator             validator;
        private String                      numericTypeFormatter = "";

        public Context(Lookup lookup) {
            this.lookup = lookup;
            validator = new ClauseValidator(lookup);
        }

        public void registerActive(String typeName) {
            if (shouldRegister(typeName)) {
                try {
                    activeType = lookup.getType(typeName);
                    aliasMap.put(typeName, typeName);
                } catch (AtlasBaseException e) {
                    validator.check(e, AtlasErrorCode.INVALID_DSL_UNKNOWN_TYPE, typeName);
                    activeType = UNKNOWN_TYPE;
                }
            }
        }

        public void registerActive(IdentifierHelper.Info info) {
            if (validator.check(StringUtils.isNotEmpty(info.getTypeName()),
                                AtlasErrorCode.INVALID_DSL_UNKNOWN_TYPE, info.getRaw())) {
                registerActive(info.getTypeName());
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

        public SelectClauseComposer getSelectClauseComposer() {
            return selectClauseComposer;
        }

        public void setSelectClauseComposer(SelectClauseComposer selectClauseComposer) {
            this.selectClauseComposer = selectClauseComposer;
        }

        public void addAlias(String alias, String typeName) {
            if (aliasMap.containsKey(alias)) {
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

        public void setNumericTypeFormatter(String formatter) {
            this.numericTypeFormatter = formatter;
        }

        public String getNumericTypeFormatter() {
            return this.numericTypeFormatter;
        }
    }

    private static class ClauseValidator {
        private final Lookup lookup;
        List<String> errorList = new ArrayList<>();

        public ClauseValidator(Lookup lookup) {
            this.lookup = lookup;
        }

        public boolean isValid(Context ctx, GremlinClause clause, IdentifierHelper.Info ia) {
            switch (clause) {
                case TRAIT:
                case ANY_TRAIT:
                case NO_TRAIT:
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

        public boolean check(boolean condition, AtlasErrorCode vm, String... args) {
            if (!condition) {
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

        private String[] getExtraSlotArgs(String[] args, String s) {
            String[] argsPlus1 = new String[args.length + 1];
            System.arraycopy(args, 0, argsPlus1, 0, args.length);
            argsPlus1[args.length] = s;
            return argsPlus1;
        }
    }
}
