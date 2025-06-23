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
package org.apache.atlas.repository.ogm;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRule;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Component
public class AtlasRuleDTO extends AbstractDataTransferObject<AtlasRule> {
    private static final Logger LOG                = LoggerFactory.getLogger(AtlasRuleDTO.class);
    public static final  String ENTITY_TYPE_NAME   = "__AtlasRule";
    public static final  String PROPERTY_RULE_NAME = "ruleName";
    public static final  String PROPERTY_RULE_EXPR = "ruleExpr";
    private static final String PROPERTY_DESC      = "desc";
    private static final String PROPERTY_ACTION    = "action";

    @Inject
    public AtlasRuleDTO(AtlasTypeRegistry typeRegistry) {
        super(typeRegistry, AtlasRule.class, ENTITY_TYPE_NAME);
    }

    @Override
    public AtlasRule from(AtlasEntity entity) {
        AtlasRule atlasRule = new AtlasRule();

        atlasRule.setGuid(entity.getGuid());
        atlasRule.setRuleName((String) entity.getAttribute(PROPERTY_RULE_NAME));
        atlasRule.setDesc((String) entity.getAttribute(PROPERTY_DESC));
        atlasRule.setAction((String) entity.getAttribute(PROPERTY_ACTION));
        String jsonRuleExpr = (String) entity.getAttribute(PROPERTY_RULE_EXPR);
        if (StringUtils.isNotEmpty(jsonRuleExpr)) {
            atlasRule.setRuleExpr(AtlasType.fromJson(jsonRuleExpr, AtlasRule.RuleExpr.class));
        }
        atlasRule.setCreatedTime(entity.getCreateTime());
        atlasRule.setUpdatedTime(entity.getUpdateTime());

        return atlasRule;
    }

    @Override
    public AtlasRule from(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        LOG.debug("==> AtlasRuleDTO.from({})", entityWithExtInfo);

        requireNonNull(entityWithExtInfo, "entity");
        AtlasRule ret = from(entityWithExtInfo.getEntity());

        LOG.debug("<== AtlasRuleDTO.from() : {}", ret);

        return ret;
    }

    @Override
    public AtlasEntity toEntity(AtlasRule atlasRule) throws AtlasBaseException {
        LOG.debug("==> AtlasRuleDTO.toEntity({})", atlasRule);

        AtlasEntity ret = getDefaultAtlasEntity(atlasRule);

        ret.setAttribute(PROPERTY_RULE_NAME, atlasRule.getRuleName());
        ret.setAttribute(PROPERTY_DESC, atlasRule.getDesc());
        ret.setAttribute(PROPERTY_ACTION, atlasRule.getAction());
        ret.setAttribute(PROPERTY_RULE_EXPR, AtlasType.toJson(atlasRule.getRuleExpr()));

        LOG.debug("<== AtlasRuleDTO.toEntity() : {}", ret);

        return ret;
    }

    @Override
    public AtlasEntity.AtlasEntityWithExtInfo toEntityWithExtInfo(AtlasRule obj) throws AtlasBaseException {
        LOG.debug("==> AtlasRuleDTO.toEntityWithExtInfo({})", obj);

        AtlasEntity.AtlasEntityWithExtInfo ret = new AtlasEntity.AtlasEntityWithExtInfo(toEntity(obj));

        LOG.debug("<== AtlasRuleDTO.toEntityWithExtInfo() : {}", ret);

        return ret;
    }

    @Override
    public Map<String, Object> getUniqueAttributes(AtlasRule obj) {
        Map<String, Object> ret = new HashMap<>();
        ret.put(PROPERTY_RULE_NAME, obj.getRuleName());
        ret.put(PROPERTY_RULE_EXPR, obj.getRuleExpr());
        return ret;
    }
}
