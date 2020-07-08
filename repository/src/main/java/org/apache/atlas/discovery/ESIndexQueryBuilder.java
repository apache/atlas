package org.apache.atlas.discovery;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasStructType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_NOT_CLASSIFIED;
import static org.apache.atlas.discovery.SearchProcessor.*;
import static org.apache.atlas.repository.Constants.*;

@SuppressWarnings("DuplicatedCode")
public class ESIndexQueryBuilder {
    SearchContext context;
    private static final Logger LOG = LoggerFactory.getLogger(ESIndexQueryBuilder.class);
    private static final Map<SearchParameters.Operator, String> OPERATOR_MAP = new HashMap<>();

    static {
        OPERATOR_MAP.put(SearchParameters.Operator.LT, "%s: [* TO %s}");

        OPERATOR_MAP.put(SearchParameters.Operator.GT, "%s: {%s TO *]");

        OPERATOR_MAP.put(SearchParameters.Operator.LTE, "%s: [* TO %s]");

        OPERATOR_MAP.put(SearchParameters.Operator.GTE, "%s: [%s TO *]");

        OPERATOR_MAP.put(SearchParameters.Operator.EQ, "%s: %s");

        OPERATOR_MAP.put(SearchParameters.Operator.NEQ, "(*:* -" + "%s: %s)");

        OPERATOR_MAP.put(SearchParameters.Operator.IN, "%s: (%s)"); // this should be a list of quoted strings

        OPERATOR_MAP.put(SearchParameters.Operator.LIKE, "%s: (%s)"); // this should be regex pattern

        OPERATOR_MAP.put(SearchParameters.Operator.STARTS_WITH, "%s: (%s*)");

        OPERATOR_MAP.put(SearchParameters.Operator.ENDS_WITH, "%s: (*%s)");

        OPERATOR_MAP.put(SearchParameters.Operator.CONTAINS, "%s: (*%s*)");

        OPERATOR_MAP.put(SearchParameters.Operator.IS_NULL, "(*:* NOT " + "%s:[* TO *])");

        OPERATOR_MAP.put(SearchParameters.Operator.NOT_NULL, "%s:[* TO *]");
    }

    ESIndexQueryBuilder(SearchContext context) {
        this.context = context;
    }

    void addClassificationTypeFilter(StringBuilder indexQuery) {
        if (indexQuery != null && CollectionUtils.isNotEmpty(context.getClassificationNames())) {
            String classificationNames = AtlasStructType.AtlasAttribute.escapeIndexQueryValue(context.getClassificationNames());
            if (indexQuery.length() != 0) {
                indexQuery.append(" AND ");
            }

            indexQuery.append("(").append(CLASSIFICATION_NAMES_KEY).append(':').append(classificationNames)
                    .append(" OR ").append(PROPAGATED_CLASSIFICATION_NAMES_KEY).append(':').append(classificationNames).append(")");
        }
    }

    void addClassificationAndSubTypesQueryFilter(StringBuilder indexQuery) {
        if (indexQuery != null && CollectionUtils.isNotEmpty(context.getClassificationTypes())) {
            String classificationTypesQryStr = context.getClassificationTypesQryStr();

            if (indexQuery.length() != 0) {
                indexQuery.append(" AND ");
            }

            indexQuery.append("(").append(CLASSIFICATION_NAMES_KEY).append(":").append(classificationTypesQryStr).append(" OR ")
                    .append(PROPAGATED_CLASSIFICATION_NAMES_KEY).append(":").append(classificationTypesQryStr).append(")");
        }
    }

    void addClassificationFilterForBuiltInTypes(StringBuilder indexQuery) {
        if (indexQuery != null && CollectionUtils.isNotEmpty(context.getClassificationTypes())) {
            if (context.getClassificationTypes().iterator().next() == MATCH_ALL_NOT_CLASSIFIED) {
                if (indexQuery.length() != 0) {
                    indexQuery.append(" AND ");
                }
                indexQuery.append("( *:* ").append("-").append(CLASSIFICATION_NAMES_KEY)
                        .append(":" + "[* TO *]").append(" AND ").append("-")
                        .append(PROPAGATED_CLASSIFICATION_NAMES_KEY)
                        .append(":" + "[* TO *]").append(")");
            }
        }
    }

    void addActiveStateQueryFilter(StringBuilder indexQuery) {
        if (context.getSearchParameters().getExcludeDeletedEntities() && indexQuery != null) {
            if (indexQuery.length() != 0) {
                indexQuery.append(" AND ");
            }
            indexQuery.append("(").append(STATE_PROPERTY_KEY).append(":" + "ACTIVE").append(")");
        }
    }

    void addTypeAndSubTypesQueryFilter(StringBuilder indexQuery, String typeAndAllSubTypesQryStr) {
        if (indexQuery != null && StringUtils.isNotEmpty(typeAndAllSubTypesQryStr)) {
            if (indexQuery.length() > 0) {
                indexQuery.append(" AND ");
            }

            indexQuery.append("(").append(Constants.TYPE_NAME_PROPERTY_KEY)
                    .append(":").append(typeAndAllSubTypesQryStr).append(")");
        }
    }

    void constructFilterQuery(StringBuilder indexQuery, Set<? extends AtlasStructType> structTypes, SearchParameters.FilterCriteria filterCriteria, Set<String> indexAttributes) {
        if (filterCriteria != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing Filters");
            }

            String filterQuery = toIndexQuery(structTypes, filterCriteria, indexAttributes, 0);

            if (org.apache.commons.lang.StringUtils.isNotEmpty(filterQuery)) {
                if (indexQuery.length() > 0) {
                    indexQuery.append(AND_STR);
                }

                indexQuery.append(filterQuery);
            }
        }
    }

    private String toIndexQuery(Set<? extends AtlasStructType> structTypes, SearchParameters.FilterCriteria criteria, Set<String> indexAttributes, int level) {
        return toIndexQuery(structTypes, criteria, indexAttributes, new StringBuilder(), level);
    }

    private String toIndexQuery(Set<? extends AtlasStructType> structTypes, SearchParameters.FilterCriteria criteria, Set<String> indexAttributes, StringBuilder sb, int level) {
        Set<String> filterAttributes = new HashSet<>(indexAttributes);

        SearchParameters.FilterCriteria.Condition condition = criteria.getCondition();
        if (condition != null && CollectionUtils.isNotEmpty(criteria.getCriterion())) {
            StringBuilder nestedExpression = new StringBuilder();

            for (SearchParameters.FilterCriteria filterCriteria : criteria.getCriterion()) {
                String nestedQuery = toIndexQuery(structTypes, filterCriteria, filterAttributes, level + 1);

                if (org.apache.commons.lang.StringUtils.isNotEmpty(nestedQuery)) {
                    if (nestedExpression.length() > 0) {
                        nestedExpression.append(SPACE_STRING).append(condition).append(SPACE_STRING);
                    }
                    nestedExpression.append(nestedQuery);
                }
            }

            boolean needSurroundingBraces = level != 0 || (condition == SearchParameters.FilterCriteria.Condition.OR && criteria.getCriterion().size() > 1);
            if (nestedExpression.length() > 0) {
                return sb.append(needSurroundingBraces ? BRACE_OPEN_STR : EMPTY_STRING)
                        .append(nestedExpression)
                        .append(needSurroundingBraces ? BRACE_CLOSE_STR : EMPTY_STRING)
                        .toString();
            } else {
                return EMPTY_STRING;
            }
        } else if (org.apache.commons.lang.StringUtils.isNotEmpty(criteria.getAttributeName())) {
            try {
                ArrayList<String> orExpQuery = new ArrayList<>();
                for (AtlasStructType structType : structTypes) {
                    String name = structType.getVertexPropertyName(criteria.getAttributeName());

                    if (filterAttributes.contains(name)) {
                        String nestedQuery = toIndexExpression(structType, criteria.getAttributeName(), criteria.getOperator(), criteria.getAttributeValue());
                        orExpQuery.add(nestedQuery);
                        filterAttributes.remove(name);
                    }
                }

                if (CollectionUtils.isNotEmpty(orExpQuery)) {
                    if (orExpQuery.size() > 1) {
                        String orExpStr = org.apache.commons.lang.StringUtils.join(orExpQuery, " " + SearchParameters.FilterCriteria.Condition.OR.name() + " ");
                        return BRACE_OPEN_STR + " " + orExpStr + " " + BRACE_CLOSE_STR;
                    } else {
                        return orExpQuery.iterator().next();
                    }
                } else {
                    return EMPTY_STRING;
                }
            } catch (AtlasBaseException e) {
                LOG.warn(e.getMessage());
            }
        }
        return EMPTY_STRING;
    }

    private String toIndexExpression(AtlasStructType type, String attrName, SearchParameters.Operator op, String attrVal) {
        String ret = EMPTY_STRING;

        try {
            if (OPERATOR_MAP.get(op) != null) {
                String qualifiedName = type.getVertexPropertyName(attrName);
                String escapeIndexQueryValue = AtlasStructType.AtlasAttribute.escapeIndexQueryValue(attrVal);

                // map '__customAttributes' 'CONTAINS' operator to 'EQ' operator (solr limitation for json serialized string search)
                // map '__customAttributes' value from 'key1=value1' to '\"key1\":\"value1\"' (escape special characters and surround with quotes)
                if (attrName.equals(CUSTOM_ATTRIBUTES_PROPERTY_KEY) && op == SearchParameters.Operator.CONTAINS) {
                    ret = String.format(OPERATOR_MAP.get(op), qualifiedName, getCustomAttributeIndexQueryValue(escapeIndexQueryValue, false));
                } else {
                    ret = String.format(OPERATOR_MAP.get(op), qualifiedName, escapeIndexQueryValue);
                }
            }
        } catch (AtlasBaseException ex) {
            LOG.warn(ex.getMessage());
        }

        return ret;
    }

    private String getCustomAttributeIndexQueryValue(String attrValue, boolean forGraphQuery) {
        String ret = null;

        if (org.apache.commons.lang.StringUtils.isNotEmpty(attrValue)) {
            int separatorIdx = attrValue.indexOf(CUSTOM_ATTR_SEPARATOR);
            String key = separatorIdx != -1 ? attrValue.substring(0, separatorIdx) : null;
            String value = key != null ? attrValue.substring(separatorIdx + 1) : null;

            if (key != null && value != null) {
                if (forGraphQuery) {
                    ret = String.format(CUSTOM_ATTR_SEARCH_FORMAT_GRAPH, key, value);
                } else {
                    ret = String.format(CUSTOM_ATTR_SEARCH_FORMAT, key, value);
                }
            } else {
                ret = attrValue;
            }
        }

        return ret;
    }

    void constructSearchSource(SearchSourceBuilder sourceBuilder, String queryString,
                               String fullTextQuery, String sortBy, org.apache.atlas.SortOrder sortOrder) {
        if (StringUtils.isNotEmpty(fullTextQuery)) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must().add(QueryBuilders.queryStringQuery(queryString));

            QueryStringQueryBuilder fullTextQueryBuilder = QueryBuilders.queryStringQuery(fullTextQuery);
            fullTextQueryBuilder.fields().put(ATLAN_ASSET_TYPE + ".integrationType", 100F);
            fullTextQueryBuilder.fields().put(ASSET_ENTITY_TYPE + ".__s_name", 50F);
            fullTextQueryBuilder.fields().put(ATLAN_ASSET_TYPE + ".displayName", 50F);
            fullTextQueryBuilder.fields().put(ASSET_ENTITY_TYPE + ".__s_owner", 20F);
            fullTextQueryBuilder.fields().put(REF_ASSET_TYPE + ".qualifiedName", 10F);
            fullTextQueryBuilder.fields().put(ATLAN_ASSET_TYPE + ".status", 5F);
            boolQueryBuilder.must().add(fullTextQueryBuilder);

            sourceBuilder.query(boolQueryBuilder);
        } else {
            sourceBuilder.query(QueryBuilders.queryStringQuery(queryString));
        }

        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));

        if (StringUtils.isNotEmpty(sortBy)) {
            SortOrder order = sortOrder == org.apache.atlas.SortOrder.ASCENDING ? SortOrder.ASC : SortOrder.DESC;
            sourceBuilder.sort(new FieldSortBuilder(sortBy).order(order));
        }
    }
}
