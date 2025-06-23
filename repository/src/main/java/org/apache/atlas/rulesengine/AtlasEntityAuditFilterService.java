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

package org.apache.atlas.rulesengine;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.SortOrder;
import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRule;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.ogm.AtlasRuleDTO;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.atlas.model.instance.AtlasEntity.KEY_STATUS;
import static org.apache.atlas.repository.Constants.ATTRIBUTE_NAME_STATE;
import static org.apache.atlas.repository.Constants.INTERNAL_PROPERTY_KEY_PREFIX;
import static org.apache.atlas.repository.ogm.AtlasRuleDTO.ENTITY_TYPE_NAME;
import static org.apache.atlas.repository.ogm.AtlasRuleDTO.PROPERTY_RULE_NAME;
import static org.apache.atlas.rulesengine.RuleAction.Result.ACCEPT;
import static org.apache.atlas.rulesengine.RuleAction.Result.DISCARD;
import static org.apache.atlas.type.AtlasStructType.UNIQUE_ATTRIBUTE_SHADE_PROPERTY_PREFIX;

@AtlasService
public class AtlasEntityAuditFilterService {
    public static final  String             ATLAS_RULE_ENTITY_NAME = "AtlasRule";
    public static final  String             ATTR_OPERATION_TYPE    = "operationType";
    public static final  String             ALL_ENTITY_TYPES       = "_ALL_ENTITY_TYPES";
    public static final  AtlasEntityType    MATCH_ALL_ENTITY_TYPES = AtlasEntityType.getEntityRoot();
    public static final  String             TYPENAME_DELIMITER     = ",";
    public static final  List<String>       externalAttributes     = new ArrayList<>(Collections.singletonList(ATTR_OPERATION_TYPE));
    static final         String             ATLAS_RULE_TYPENAME    = "__AtlasRule";
    private static final Logger             LOG                    = LoggerFactory.getLogger(AtlasAuditService.class);
    final                AtlasTypeRegistry  typeRegistry;
    private final        DataAccess         dataAccess;
    private final        AtlasRuleDTO       ruleDTO;
    private final        boolean            isEntityAuditCustomFilterEnabled;
    private final        String             defaultEntityAuditFilterAction;
    private              JsonLogicConverter jsonLogicConverter;
    private              boolean            isDiscardByDefault;

    @Inject
    public AtlasEntityAuditFilterService(@Lazy DataAccess dataAccess, AtlasTypeRegistry atlasTypeRegistry, AtlasRuleDTO ruleDTO) {
        this.dataAccess                       = dataAccess;
        this.typeRegistry                     = atlasTypeRegistry;
        this.ruleDTO                          = ruleDTO;
        this.isEntityAuditCustomFilterEnabled = AtlasConfiguration.ENTITY_AUDIT_FILTER_ENABLED.getBoolean();
        this.defaultEntityAuditFilterAction   = AtlasConfiguration.DEFAULT_ENTITY_AUDIT_FILTER_ACTION.getString();
        if (isEntityAuditCustomFilterEnabled()) {
            isDiscardByDefault = setDiscardByDefault();
            jsonLogicConverter = new JsonLogicConverter();
            LOG.info("AtlasEntityAuditFilterService is initialized");
        } else {
            LOG.info("AtlasEntityAuditFilterService is disabled");
        }
    }

    public static boolean isDuplicateRuleExpr(List<AtlasRule.RuleExprObject.Criterion> criterion) {
        Object[] criterionList = criterion.toArray();
        return Arrays.stream(criterionList).distinct().count() != criterionList.length;
    }

    public boolean isDiscardByDefault() {
        return isDiscardByDefault;
    }

    public boolean isEntityAuditCustomFilterEnabled() {
        return isEntityAuditCustomFilterEnabled;
    }

    public String getDefaultEntityAuditFilterAction() {
        return defaultEntityAuditFilterAction;
    }

    public JsonLogicConverter getJsonLogicConverter() {
        return jsonLogicConverter;
    }

    public RuleAction getDefaultAction() {
        return AtlasRuleUtils.getRuleActionFromString(defaultEntityAuditFilterAction);
    }

    @GraphTransaction
    public List<AtlasRule> fetchRules() throws AtlasBaseException {
        LOG.debug("==> AtlasEntityAuditFilterService.fetchRules()");

        List<AtlasRule>                      rules;
        List<String>                         ruleGuids = AtlasGraphUtilsV2.findEntityGUIDsByType(ATLAS_RULE_TYPENAME, SortOrder.ASCENDING);
        AtlasEntity.AtlasEntitiesWithExtInfo ruleEntities;

        if (CollectionUtils.isNotEmpty(ruleGuids)) {
            ruleEntities = dataAccess.getAtlasEntityStore().getByIds(ruleGuids, true, false);
            rules        = new ArrayList<>();
            for (AtlasEntity ruleEntity : ruleEntities.getEntities()) {
                AtlasRule rule = ruleDTO.from(ruleEntity);
                rules.add(rule);
            }
        } else {
            rules = Collections.emptyList();
        }

        LOG.debug("<== AtlasEntityAuditFilterService.fetchRules() : {}", rules);

        return AtlasRuleUtils.getSortedRules(rules);
    }

    @GraphTransaction
    public AtlasRule createRule(AtlasRule atlasRule) throws AtlasBaseException {
        LOG.debug("==> AtlasEntityAuditFilterService.createRule({})", atlasRule);

        if (Objects.isNull(atlasRule)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Rule definition missing");
        }

        if (StringUtils.isEmpty(atlasRule.getRuleName())) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE, ATLAS_RULE_ENTITY_NAME, "ruleName");
        }
        if (StringUtils.isEmpty(atlasRule.getAction())) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE, ATLAS_RULE_ENTITY_NAME, "action");
        }
        if (Objects.isNull(atlasRule.getRuleExpr())) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE, ATLAS_RULE_ENTITY_NAME, "ruleExpr");
        }

        validateRuleAction(atlasRule.getAction());
        validateRuleName(atlasRule.getRuleName());
        validateRuleExprExists(atlasRule.getRuleExpr());
        validateRuleExprFormat(atlasRule.getRuleExpr());

        AtlasRule storeObject = dataAccess.save(atlasRule);

        LOG.debug("<== AtlasEntityAuditFilterService.createRule() : {}", storeObject);

        return storeObject;
    }

    public Set<String> getTypesAndSubtypes(Set<AtlasEntityType> entityTypes) {
        Set<String> typeAndSubTypes = new HashSet<>();
        if (CollectionUtils.isNotEmpty(entityTypes)) {
            for (AtlasEntityType entityType : entityTypes) {
                if (entityType.equals(MATCH_ALL_ENTITY_TYPES)) {
                    typeAndSubTypes = Collections.emptySet();
                    break;
                } else {
                    Set<String> allTypes = entityType.getTypeAndAllSubTypes();
                    typeAndSubTypes.addAll(allTypes);
                }
            }
        } else {
            typeAndSubTypes = Collections.emptySet();
        }

        return typeAndSubTypes;
    }

    @GraphTransaction
    public AtlasRule updateRule(AtlasRule atlasRule) throws AtlasBaseException {
        LOG.debug("==> AtlasEntityAuditFilterService.updateRule({})", atlasRule);

        if (Objects.isNull(atlasRule)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Rule is null/empty");
        }

        AtlasRule storeObject = dataAccess.load(atlasRule);

        if (StringUtils.isEmpty(atlasRule.getDesc())) {
            atlasRule.setDesc(storeObject.getDesc());
        }

        if (StringUtils.isNotEmpty(atlasRule.getRuleName()) && !storeObject.getRuleName().equals(atlasRule.getRuleName())) {
            validateRuleName(atlasRule.getRuleName());
        } else {
            atlasRule.setRuleName(storeObject.getRuleName());
        }

        if (StringUtils.isNotEmpty(atlasRule.getAction())) {
            validateRuleAction(atlasRule.getAction());
        } else {
            atlasRule.setAction(storeObject.getAction());
        }

        if (!Objects.isNull(atlasRule.getRuleExpr())) {
            validateRuleExprFormat(atlasRule.getRuleExpr());
        } else {
            atlasRule.setRuleExpr(storeObject.getRuleExpr());
        }

        storeObject = dataAccess.save(atlasRule);

        LOG.debug("<== AtlasEntityAuditFilterService.updateRule() : {}", storeObject);

        return storeObject;
    }

    @GraphTransaction
    public EntityMutationResponse deleteRule(String ruleGuid) throws AtlasBaseException {
        LOG.debug("==> AtlasEntityAuditFilterService.deleteRule({})", ruleGuid);

        if (Objects.isNull(ruleGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "RuleGuid is null/empty");
        }

        AtlasRule storeObject = new AtlasRule();
        storeObject.setGuid(ruleGuid);
        dataAccess.load(storeObject); //this will check and revert with exception if the rule is already deleted
        EntityMutationResponse ret = dataAccess.delete(ruleGuid);

        LOG.debug("<== AtlasEntityAuditFilterService.deleteRule()");

        return ret;
    }

    public EntityMutationResponse deleteRules(List<String> ruleGuids) throws AtlasBaseException {
        LOG.debug("==> AtlasEntityAuditFilterService.deleteRules({})", Arrays.toString(ruleGuids.toArray()));

        if (CollectionUtils.isEmpty(ruleGuids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }
        List<String> guidList = new ArrayList<>();
        for (String ruleGuid : ruleGuids) {
            AtlasRule storeObject = new AtlasRule();
            storeObject.setGuid(ruleGuid);
            dataAccess.load(storeObject);
            guidList.add(ruleGuid);
        }
        EntityMutationResponse ret = dataAccess.delete(guidList);

        LOG.debug("<== AtlasEntityAuditFilterService.deleteRules()");

        return ret;
    }

    public EntityMutationResponse deleteAllRules() throws AtlasBaseException {
        LOG.debug("==> AtlasEntityAuditFilterService.deleteAllRules()");

        List<String> ruleGuids = AtlasGraphUtilsV2.findEntityGUIDsByType(ATLAS_RULE_TYPENAME, SortOrder.ASCENDING);
        if (CollectionUtils.isEmpty(ruleGuids)) {
            throw new AtlasBaseException(AtlasErrorCode.EMPTY_RESULTS, "Rules");
        }

        for (String ruleGuid : ruleGuids) {
            AtlasRule storeObject = new AtlasRule();
            storeObject.setGuid(ruleGuid);
            dataAccess.load(storeObject);
        }
        EntityMutationResponse ret = dataAccess.delete(ruleGuids);

        LOG.debug("<== AtlasEntityAuditFilterService.deleteAllRules()");

        return ret;
    }

    public Set<String> getAllMatchingEntityNames(String singleTypeName, boolean includeSubTypes) {
        Set<String> matchingTypeNames = null;
        if (!includeSubTypes) {
            if (!singleTypeName.contains("*")) {
                return Collections.singleton(singleTypeName);
            }
            matchingTypeNames = getAllMatchingEntityTypesAsStringSet(singleTypeName);
        } else {
            Set<AtlasEntityType> matchingEntityTypes = getAllMatchingEntityTypesAsTypeSet(singleTypeName);
            if (matchingEntityTypes != null) {
                matchingTypeNames = getTypesAndSubtypes(matchingEntityTypes);
            }
        }

        return matchingTypeNames;
    }

    Set<AtlasEntityType> getEntityTypes(String typeName) throws AtlasBaseException {
        Set<AtlasEntityType> entityTypes = null;
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(typeName)) {
            Set<String> typeNames = Stream.of(typeName.trim().split(TYPENAME_DELIMITER)).collect(Collectors.toSet());
            if (typeNames.size() > 1 && typeNames.contains(ALL_ENTITY_TYPES)) {
                throw new AtlasBaseException(ALL_ENTITY_TYPES + " can not be included with any other entity type");
            }

            entityTypes = new HashSet<>();
            Set<String> invalidEntityTypes = new HashSet<>();

            for (String name : typeNames) {
                Set<AtlasEntityType> matchingEntityTypes = getAllMatchingEntityTypesAsTypeSet(name);
                if (matchingEntityTypes != null) {
                    entityTypes.addAll(matchingEntityTypes);
                } else {
                    invalidEntityTypes.add(name);
                }
            }

            if (invalidEntityTypes.size() > 0) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, String.join(TYPENAME_DELIMITER, invalidEntityTypes));
            }
        }
        return entityTypes;
    }

    Set<String> getEntityTypeNames(String typeName, boolean includeSubTypes) {
        Set<String> entityTypeNames = null;
        if (StringUtils.isNotEmpty(typeName)) {
            //all mix formed multiple typenames received - with or without wildcard;
            Set<String> typeNames = Stream.of(typeName.trim().split(TYPENAME_DELIMITER)).collect(Collectors.toSet());

            entityTypeNames = new HashSet<>();
            for (String name : typeNames) {
                Set<String> matchingEntityTypes = getAllMatchingEntityNames(name, includeSubTypes);
                entityTypeNames.addAll(matchingEntityTypes);
            }
        }

        return entityTypeNames;
    }

    private boolean setDiscardByDefault() {
        RuleAction.Result result = ACCEPT;
        if (getDefaultAction() != null) {
            result = getDefaultAction().getResult();
        }
        return (result == DISCARD);
    }

    private void validateRuleName(String ruleName) throws AtlasBaseException {
        AtlasVertex vertex = AtlasGraphUtilsV2.findByUniqueAttributes(typeRegistry.getEntityTypeByName(ATLAS_RULE_TYPENAME), new HashMap<String, Object>() {
            {
                put(PROPERTY_RULE_NAME, ruleName);
            }
        });
        if (Objects.nonNull(vertex)) {
            throw new AtlasBaseException(AtlasErrorCode.RULE_NAME_ALREADY_EXISTS, ruleName);
        }
    }

    private void validateRuleAction(String action) throws AtlasBaseException {
        if (StringUtils.isNotEmpty(action)) {
            for (RuleAction.Result res : RuleAction.Result.values()) {
                if (res.name().equals(action)) {
                    return;
                }
            }
            throw new AtlasBaseException(AtlasErrorCode.INVALID_RULE_ACTION);
        }
    }

    private void validateRuleExprFormat(AtlasRule.RuleExpr ruleExpr) throws AtlasBaseException {
        if (ruleExpr == null) {
            return;
        }

        List<AtlasRule.RuleExprObject> allExpressions        = ruleExpr.getRuleExprObjList();
        List<List<String>>             recordedTypeNamesList = new ArrayList<>();

        for (AtlasRule.RuleExprObject ruleExprObj : allExpressions) {
            validateTypeName(ruleExprObj.getTypeName(), recordedTypeNamesList);

            AtlasRule.Condition                      condition = ruleExprObj.getCondition();
            List<AtlasRule.RuleExprObject.Criterion> criterion = ruleExprObj.getCriterion();
            validateConditionAndCriteria(condition, criterion);

            validateAttributes(ruleExprObj, condition, criterion);
        }
    }

    private void validateTypeName(String typeName, List<List<String>> recordedTypeNamesList) throws AtlasBaseException {
        if (Strings.isNullOrEmpty(typeName) || "null".equals(typeName)) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_TYPENAME_IN_RULE_EXPR);
        }

        if (isDuplicateTypeNameValue(recordedTypeNamesList, typeName)) {
            throw new AtlasBaseException(AtlasErrorCode.DUPLICATE_TYPENAME_IN_RULE_EXPR, typeName);
        }

        recordedTypeNamesList.add(Arrays.asList(typeName.split(",")));

        getEntityTypes(typeName);
    }

    private void validateConditionAndCriteria(AtlasRule.Condition condition, List<AtlasRule.RuleExprObject.Criterion> criterion) throws AtlasBaseException {
        if (condition != null && CollectionUtils.isEmpty(criterion)) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_CRITERIA_CONDITION, "criteria");
        }
        if (CollectionUtils.isNotEmpty(criterion) && condition == null) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_CRITERIA_CONDITION, "condition");
        }
    }

    private void validateAttributes(AtlasRule.RuleExprObject ruleExprObj, AtlasRule.Condition condition, List<AtlasRule.RuleExprObject.Criterion> criterion) throws AtlasBaseException {
        Set<AtlasEntityType> entityTypes = getEntityTypes(ruleExprObj.getTypeName());

        for (AtlasEntityType entityType : entityTypes) {
            if (condition != null && CollectionUtils.isNotEmpty(criterion)) {
                validateCriteriaList(entityType, criterion);
            } else {
                validateExpression(entityType, ruleExprObj.getOperator(), ruleExprObj.getAttributeName(), ruleExprObj.getAttributeValue());
            }
        }
    }

    private boolean isDuplicateTypeNameValue(List<List<String>> recordedTypeNamesList, String typeName) {
        return AtlasRuleUtils.isDuplicateList(recordedTypeNamesList, Arrays.asList(typeName.split(",")));
    }

    private void validateExternalAttribute(String attrName, String attrValue, AtlasRule.RuleExprObject.Operator operator) throws AtlasBaseException {
        if (ATTR_OPERATION_TYPE.equals(attrName)) {
            EntityAuditEventV2.EntityAuditActionV2[] enumConstants = EntityAuditEventV2.EntityAuditActionV2.class.getEnumConstants();
            if (isValidOperator(enumConstants, operator, attrValue)) {
                return;
            }
        }

        throw new AtlasBaseException(AtlasErrorCode.INVALID_OPERATOR_ON_ATTRIBUTE, operator.getSymbol(), ATTR_OPERATION_TYPE);
    }

    private boolean isValidOperator(EntityAuditEventV2.EntityAuditActionV2[] enumConstants, AtlasRule.RuleExprObject.Operator operator, String attrValue) {
        switch (operator) {
            case EQ:
                return Arrays.stream(enumConstants).anyMatch(e -> e.name().equals(attrValue));
            case STARTS_WITH:
                return Arrays.stream(enumConstants).anyMatch(e -> (boolean) AtlasRuleUtils.startsWithFunc.apply(new String[] {e.name(), attrValue}));
            case ENDS_WITH:
                return Arrays.stream(enumConstants).anyMatch(e -> (boolean) AtlasRuleUtils.endsWithFunc.apply(new String[] {e.name(), attrValue}));
            case CONTAINS:
                return Arrays.stream(enumConstants).anyMatch(e -> (boolean) AtlasRuleUtils.containsFunc.apply(new String[] {e.name(), attrValue}));
            case CONTAINS_IGNORECASE:
                return Arrays.stream(enumConstants).anyMatch(e -> (boolean) AtlasRuleUtils.containsIgnoreCaseFunc.apply(new String[] {e.name(), attrValue}));
            case NOT_CONTAINS:
                return Arrays.stream(enumConstants).anyMatch(e -> (boolean) AtlasRuleUtils.notContainsFunc.apply(new String[] {e.name(), attrValue}));
            case NOT_CONTAINS_IGNORECASE:
                return Arrays.stream(enumConstants).anyMatch(e -> (boolean) AtlasRuleUtils.notContainsIgnoreCaseFunc.apply(new String[] {e.name(), attrValue}));
            default:
                return false;
        }
    }

    private AtlasEntityType getEntityType(String entityName) {
        return StringUtils.equals(entityName, ALL_ENTITY_TYPES) ? MATCH_ALL_ENTITY_TYPES :
                typeRegistry.getEntityTypeByName(entityName);
    }

    private Set<String> getAllMatchingEntityTypesAsStringSet(String entityName) {
        Collection<String> allTypeNamesSet = typeRegistry.getAllEntityDefNames();

        return allTypeNamesSet.stream()
                .filter(strTypeName -> AtlasRuleUtils.match(entityName, strTypeName))
                .filter(Objects::nonNull).collect(Collectors.toSet());
    }

    private Set<AtlasEntityType> getAllMatchingEntityTypesAsTypeSet(String entityName) {
        if (!entityName.contains("*")) {
            AtlasEntityType entityType = getEntityType(entityName);
            return entityType == null ? null : Collections.singleton(entityType);
        }

        Set<String>          matchingTypeNames = getAllMatchingEntityTypesAsStringSet(entityName);
        Set<AtlasEntityType> entityTypes       = null;
        if (matchingTypeNames.size() > 0) {
            entityTypes = matchingTypeNames.stream()
                    .map(n -> typeRegistry.getEntityTypeByName(n))
                    .filter(Objects::nonNull).collect(Collectors.toSet());
        }

        return entityTypes;
    }

    private void validateOperator(AtlasRule.RuleExprObject.Operator operator) throws AtlasBaseException {
        if (operator == null) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_OPERATOR_IN_RULE_EXPR_CRITERIA);
        }
    }

    private void validateRuleExprExists(AtlasRule.RuleExpr ruleExpr) throws AtlasBaseException {
        AtlasVertex vertex = AtlasGraphUtilsV2.findByUniqueAttributes(typeRegistry.getEntityTypeByName(ATLAS_RULE_TYPENAME), new HashMap<String, Object>() {
            {
                put(AtlasRuleDTO.PROPERTY_RULE_EXPR, AtlasType.toJson(ruleExpr));
            }
        });
        if (Objects.nonNull(vertex)) {
            String propName = ENTITY_TYPE_NAME + "." + UNIQUE_ATTRIBUTE_SHADE_PROPERTY_PREFIX + PROPERTY_RULE_NAME;
            String ruleName = vertex.getProperty(propName, String.class);
            throw new AtlasBaseException(AtlasErrorCode.RULE_EXPRESSION_ALREADY_EXISTS, ruleName);
        }
    }

    private void validateCriteriaList(AtlasEntityType entityType, List<AtlasRule.RuleExprObject.Criterion> criteriaList) throws AtlasBaseException {
        if (isDuplicateRuleExpr(criteriaList)) {
            throw new AtlasBaseException(AtlasErrorCode.DUPLICATE_CONDITION_IN_SAME_RULE_EXPR);
        }
        for (AtlasRule.RuleExprObject.Criterion criterion : criteriaList) {
            validateCriteria(entityType, criterion);
        }
    }

    private void validateCriteria(AtlasEntityType entityType, AtlasRule.RuleExprObject.Criterion criteria) throws AtlasBaseException {
        AtlasRule.Condition condition = criteria.getCondition();
        if (condition != null && CollectionUtils.isNotEmpty(criteria.getCriterion())) {
            validateCriteriaList(entityType, criteria.getCriterion());
        } else {
            validateExpression(entityType, criteria.getOperator(), criteria.getAttributeName(), criteria.getAttributeValue());
        }
    }

    private void validateExpression(AtlasEntityType entityType, AtlasRule.RuleExprObject.Operator op, String attrName, String attrVal) throws AtlasBaseException {
        if (op != null || StringUtils.isNotEmpty(attrName) || StringUtils.isNotEmpty(attrVal)) {
            validateOperator(op);
            validateAttribute(entityType, attrName, attrVal, op);
        }
    }

    private void validateAttribute(AtlasEntityType entityType, String attributeName, String attributeValue, AtlasRule.RuleExprObject.Operator operator) throws AtlasBaseException {
        if (Strings.isNullOrEmpty(attributeName) || "null".equals(attributeName)) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_ATTRIBUTE_NAME_IN_RULE_EXPR);
        }
        if (operator.isBinary() && (Strings.isNullOrEmpty(attributeValue) || "null".equals(attributeValue))) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_ATTRIBUTE_VALUE_IN_RULE_EXPR, attributeName);
        }
        if (externalAttributes.contains(attributeName)) {
            validateExternalAttribute(attributeName, attributeValue, operator);
        } else {
            validateAttribute(entityType, attributeName);
        }
    }

    private void validateAttribute(final AtlasEntityType entityType, String attributeName) throws AtlasBaseException {
        if (StringUtils.isNotEmpty(attributeName) && (entityType == null || entityType.getAttributeType(attributeName) == null)) {
            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, "NULL");
            }
            String name = entityType.getTypeName();
            if (name.equals(MATCH_ALL_ENTITY_TYPES.getTypeName())) {
                name = ALL_ENTITY_TYPES;
            }
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attributeName, name);
        }
    }

    class JsonLogicConverter {
        private static final String PROPERTY_KEY_TYPENAME        = "typeName";
        private static final String PROPERTY_KEY_INCLUDESUBTYPES = "includeSubTypes";
        private static final String PROPERTY_KEY_CONDITION       = "condition";
        private static final String PROPERTY_KEY_CRITERION       = "criterion";
        private static final String PROPERTY_KEY_ATTRIBUTENAME   = "attributeName";
        private static final String PROPERTY_KEY_ATTRIBUTEVALUE  = "attributeValue";
        private static final String PROPERTY_KEY_OPERATOR        = "operator";
        private final        Gson   gson                         = new Gson();

        private JsonLogicConverter() {
        }

        public String convertToJsonLogic(String ruleExprJson) {
            JsonArray     ruleExprArray = gson.fromJson(ruleExprJson, JsonArray.class);
            StringBuilder finalResult   = new StringBuilder();
            String        beginExpr     = "{\"" + AtlasRule.Condition.OR.name().toLowerCase() + "\": [";
            String        endExpr       = "]}";

            if (ruleExprArray.size() > 1) {
                finalResult.append(beginExpr);
            }

            for (JsonElement ruleExprObj : ruleExprArray) {
                String ruleExprObjJLStr = convertRuleExprObj((JsonObject) ruleExprObj);
                finalResult.append(ruleExprObjJLStr).append(",");
            }
            finalResult.deleteCharAt(finalResult.length() - 1);
            if (ruleExprArray.size() > 1) {
                finalResult.append(endExpr);
            }
            return finalResult.toString();
        }

        private String convertRuleExprObj(JsonObject ruleExprObj) {
            String  op              = extractStringProperty(ruleExprObj, PROPERTY_KEY_CONDITION, PROPERTY_KEY_OPERATOR);
            String  typeNameValue   = extractStringProperty(ruleExprObj, PROPERTY_KEY_TYPENAME);
            boolean includeSubTypes = extractBooleanProperty(ruleExprObj, PROPERTY_KEY_INCLUDESUBTYPES);

            if (op == null && !ALL_ENTITY_TYPES.equals(typeNameValue)) {
                return getMatchingTypeNameCondition(typeNameValue, includeSubTypes);
            }

            String typeNameConditionStr = null;
            String conditionRes         = null;
            String criteriaRes          = null;

            if (typeNameValue != null && !ALL_ENTITY_TYPES.equals(typeNameValue)) {
                typeNameConditionStr = getMatchingTypeNameCondition(typeNameValue, includeSubTypes);
            }

            if ("and".equalsIgnoreCase(op) || "or".equalsIgnoreCase(op)) {
                JsonArray criterion = ruleExprObj.getAsJsonArray(PROPERTY_KEY_CRITERION);
                String    result    = convertCriterionArray(criterion);
                conditionRes = "{\"" + op.toLowerCase() + "\": [" + result + "]}";
            } else {
                criteriaRes = formatSingleCriterion(ruleExprObj);
            }

            if (typeNameConditionStr != null) {
                if (conditionRes != null) {
                    return "{\"and\": [" + typeNameConditionStr + ",  " + conditionRes + "]}";
                } else {
                    return "{\"and\": [" + typeNameConditionStr + ",  " + criteriaRes + "]}";
                }
            } else if (conditionRes != null) {
                return conditionRes;
            } else {
                return criteriaRes;
            }
        }

        private String extractStringProperty(JsonObject jsonObject, String... propertyKeys) {
            for (String propertyKey : propertyKeys) {
                if (jsonObject.has(propertyKey)) {
                    return jsonObject.get(propertyKey).getAsString();
                }
            }
            return null;
        }

        private boolean extractBooleanProperty(JsonObject jsonObject, String propertyKey) {
            if (jsonObject.has(propertyKey)) {
                return jsonObject.get(propertyKey).getAsBoolean();
            }
            return true;
        }

        private String convertCriterionArray(JsonArray criterion) {
            StringBuilder result = new StringBuilder();
            for (JsonElement criteria : criterion) {
                JsonObject subCondition = criteria.getAsJsonObject();
                String     subResult    = convertRuleExprObj(subCondition);
                result.append(subResult).append(",");
            }
            if (result.length() > 0) {
                result.deleteCharAt(result.length() - 1);
            }
            return result.toString();
        }

        private String formatSingleCriterion(JsonObject ruleExprObj) {
            String op          = ruleExprObj.get(PROPERTY_KEY_OPERATOR).getAsString();
            String attrName    = formatAttrName(ruleExprObj.get(PROPERTY_KEY_ATTRIBUTENAME).getAsString());
            String criteriaRes = "{\"" + op + "\": [{\"var\":\"" + attrName + "\"}";
            if (ruleExprObj.has(PROPERTY_KEY_ATTRIBUTEVALUE)) {
                String attrVal = ruleExprObj.get(PROPERTY_KEY_ATTRIBUTEVALUE).getAsString();
                criteriaRes += "," + attrVal;
            }
            criteriaRes += "]}";
            return criteriaRes;
        }

        private String getMatchingTypeNameCondition(String typeNameValue, boolean includeSubTypes) {
            Set<String> entityTypeNames = getEntityTypeNames(typeNameValue, includeSubTypes);

            if (entityTypeNames.size() == 1) {
                return "{\"contains\" : [{\"var\":\"" + PROPERTY_KEY_TYPENAME + "\"}, \"" + entityTypeNames.stream().findAny().get() + "\"]}";
            }

            StringBuilder typeNameConditionStr = new StringBuilder(" {\"in\":[{\"var\":\"" + PROPERTY_KEY_TYPENAME + "\"}, [");

            for (String typeName : entityTypeNames) {
                typeNameConditionStr.append("\"").append(typeName).append("\"").append(",");
            }
            typeNameConditionStr.deleteCharAt(typeNameConditionStr.length() - 1);
            typeNameConditionStr.append("] ]}");

            return typeNameConditionStr.toString();
        }

        private String formatAttrName(String attributeName) {
            if (attributeName.startsWith(INTERNAL_PROPERTY_KEY_PREFIX)) {
                attributeName = attributeName.substring(2);
                if (ATTRIBUTE_NAME_STATE.equals(attributeName)) {
                    attributeName = KEY_STATUS;
                }
            } else if (!externalAttributes.contains(attributeName)) {
                attributeName = "attributes." + attributeName;
            }
            return attributeName;
        }
    }
}
