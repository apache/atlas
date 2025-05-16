// @ts-nocheck

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

import {
  Popover,
  Stack,
  Typography,
  FormGroup,
  FormControlLabel
} from "@mui/material";

import { useEffect, useState } from "react";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  CustomButton
} from "../muiComponents";
import { useLocation, useNavigate } from "react-router-dom";
import {
  customSortBy,
  getNestedSuperTypeObj,
  getUrlState,
  globalSearchFilterInitialQuery,
  isEmpty
} from "@utils/Utils";
import type { Field, RuleGroupType } from "react-querybuilder";
import { toFullOption } from "react-querybuilder";
import "react-querybuilder/dist/query-builder.scss";
import { useAppSelector } from "@hooks/reducerHook";
import { cloneDeep } from "@utils/Helper";
import { getObjDef } from "@views/Administrator/Audits/AuditsFilter/AuditFiltersFields";
import { attributeFilter } from "@utils/CommonViewFunction";
import moment from "moment";
import RelationshipFilters from "./RelationshipFilters";
import TypeFilters from "./TypeFilters/TypeFilters";
import TagFilters from "./TagFilters/TagFilters";
import { AntSwitch } from "@utils/Muiutils";
import "@styles/filterQueryBuilder.scss";
import "@styles/filterQuery.scss";

const Filters = ({
  popoverId,
  filtersOpen,
  filtersPopover,
  handleCloseFilterPopover,
  setUpdateTable
}: any) => {
  const location = useLocation();
  const navigate = useNavigate();
  const initialQuery: RuleGroupType = { combinator: "and", rules: [] };
  const searchParams: any = new URLSearchParams(location.search);
  const typeParams = searchParams.get("type");
  const tagParams = searchParams.get("tag");
  const relationshipParams = searchParams.get("relationshipName");
  const entityFilterParams = searchParams.get("entityFilters");
  const [checkedEntities, setCheckedEntities] = useState<any>(
    !isEmpty(searchParams.get("includeDE"))
      ? searchParams.get("includeDE")
      : false
  );
  const [checkedSubClassifications, setCheckedSubClassifications] =
    useState<any>(
      !isEmpty(searchParams.get("excludeSC"))
        ? searchParams.get("excludeSC")
        : false
    );
  const [checkedSubTypes, setCheckedSubTypes] = useState<any>(
    !isEmpty(searchParams.get("excludeST"))
      ? searchParams.get("excludeST")
      : false
  );
  const [typeQuery, setTypeQuery] = useState(
    !isEmpty(globalSearchFilterInitialQuery.getQuery()?.entityFilters) &&
      !isEmpty(entityFilterParams)
      ? globalSearchFilterInitialQuery.getQuery()?.entityFilters
      : initialQuery
  );
  const [classificationQuery, setClassificationQuery] = useState(
    !isEmpty(globalSearchFilterInitialQuery.getQuery()?.tagFilters)
      ? globalSearchFilterInitialQuery.getQuery()?.tagFilters
      : initialQuery
  );
  const [relationshipQuery, setRelationshipQuery] = useState(
    !isEmpty(globalSearchFilterInitialQuery.getQuery()?.relationshipFilters)
      ? globalSearchFilterInitialQuery.getQuery()?.relationshipFilters
      : initialQuery
  );
  const { entityData }: any = useAppSelector((state) => state.entity);
  const { classificationData }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { enumObj = {} }: any = useAppSelector((state: any) => state.enum);
  const { allEntityTypesData }: any = useAppSelector(
    (state: any) => state.allEntityTypes
  );
  const { businessMetaData }: any = useAppSelector(
    (state: any) => state.businessMetaData
  );
  const { classificationDefs } = classificationData || {};
  const { entityDefs = {} } = entityData || {};
  const { enumDefs = {} } = enumObj?.data || {};
  const { businessMetadataDefs = {} } = businessMetaData || {};

  let allDataObj = {
    entitys: entityDefs,
    enums: enumDefs,
    tags: classificationDefs,
    businessMetadata: businessMetadataDefs
  };

  const handleSwitchChangeEntities = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    searchParams.set("includeDE", event.target.checked);
    setCheckedEntities(event.target.checked);
  };

  const handleSwitchChangeSubClassification = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    searchParams.set("excludeSC", event.target.checked);
    setCheckedSubClassifications(event.target.checked);
  };

  const handleSwitchChangeSubTypes = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    searchParams.set("excludeST", event.target.checked);
    setCheckedSubTypes(event.target.checked);
  };

  const paramsObject: Record<string, any> = {};
  searchParams.forEach((value: any, key: string | number) => {
    paramsObject[key] = value;
  });

  const { type, tag, tagFilters, entityFilters } = paramsObject || {};

  let obj = {
    value: paramsObject,
    relationship: getUrlState.isRelationSearch() ? true : false,
    searchVent: {}
  };

  let allEntityTypeAtrr = !isEmpty(allEntityTypesData)
    ? allEntityTypesData
    : {};

  let entityTypeObj =
    !isEmpty(entityDefs) && !isEmpty(type)
      ? entityDefs.find((entity: { name: string }) => {
          return entity.name == type;
        })
      : {};

  let attrEntityTypeObj = entityTypeObj;
  let entityAttributeLength = "";
  if (attrEntityTypeObj) {
    attrEntityTypeObj = getNestedSuperTypeObj({
      data: attrEntityTypeObj,
      collection: entityDefs,
      attrMerge: true
    });
    entityAttributeLength = attrEntityTypeObj.length;
  }
  const fields = () => {
    let filters = [];
    let isGroupView = true;
    let rules_widgets = null;
    let systemAttrArr;

    if (!isEmpty(paramsObject)) {
      rules_widgets = attributeFilter.extractUrl({
        value: undefined,
        formatDate: true
      });
    }
    for (const attrObj in attrEntityTypeObj) {
      let returnObj: any = getObjDef(
        allDataObj,
        attrEntityTypeObj[attrObj],
        rules_widgets,
        isGroupView,
        type + " Attribute"
      );
      if (returnObj) {
        filters.push(returnObj);
      }
    }

    let sortMap: Record<string, number> = {
      __guid: 1,
      __typeName: 2,
      __timestamp: 3,
      __modificationTimestamp: 4,
      __createdBy: 5,
      __modifiedBy: 6,
      __isIncomplete: 7,
      __classificationNames: 9,
      __propagatedClassificationNames: 10,
      __labels: 11,
      __customAttributes: 12
    };
    if (type) {
      sortMap["__state"] = 8;
    } else {
      sortMap["__entityStatus"] = 8;
    }
    systemAttrArr = !isEmpty(allEntityTypeAtrr?.attributeDefs)
      ? Object.values(allEntityTypeAtrr?.attributeDefs).sort(function (
          a: any,
          b: any
        ): any {
          return sortMap[a.name] - sortMap[b.name];
        })
      : [];

    for (const sysAttr of systemAttrArr) {
      let returnObj: any = getObjDef(
        allDataObj,
        sysAttr,
        rules_widgets,
        isGroupView,
        "System Attribute",
        true
      );
      if (returnObj) {
        filters.push(returnObj);
      }
    }
    const pushBusinessMetadataFilter = function (
      sortedAttributes: any,
      businessMetadataKey: string
    ) {
      for (let sortedAttr of sortedAttributes) {
        let returnObj: any = getObjDef(
          allDataObj,
          sortedAttr,
          rules_widgets,
          isGroupView,
          "Business Attributes: " + businessMetadataKey
        );
        if (returnObj) {
          returnObj.id = businessMetadataKey + "." + returnObj.id;
          returnObj.label = returnObj.label;
          returnObj.data = { entityType: "businessMetadata" };
          filters.push(returnObj);
        }
      }
    };

    if (typeParams == "_ALL_ENTITY_TYPES") {
      for (const bm of businessMetadataDefs) {
        let sortedAttributes = bm.attributeDefs;
        sortedAttributes = customSortBy(sortedAttributes, ["name"]);
        filters.push(pushBusinessMetadataFilter(sortedAttributes, bm.name));
      }
    } else {
      let businessMetadataAttributeDefs = null;
      if (entityTypeObj) {
        businessMetadataAttributeDefs = entityTypeObj.businessAttributeDefs;
      }
      if (businessMetadataAttributeDefs) {
        for (const attributes in businessMetadataAttributeDefs) {
          let sortedAttributes = customSortBy(
            businessMetadataAttributeDefs[attributes],
            ["name"]
          );
          pushBusinessMetadataFilter(sortedAttributes, attributes);
        }
      }
    }
    return (filters satisfies Field[]).map((o) => toFullOption(o));
  };

  const groupedFields =
    fields()?.reduce(
      (
        acc: { [x: string]: any[] },
        field: { group: string | number | undefined }
      ) => {
        if (field && field.group !== undefined) {
          if (!acc[field.group]) {
            acc[field.group] = [];
          }
          acc[field.group].push(field);
        }
        return acc;
      },
      {}
    ) || {};

  const fieldsObj = Object.keys(groupedFields).map((group) => ({
    label: group,
    options: groupedFields[group]
  }));
  function processCombinators(obj: {
    [x: string]: any;
    combinator: any;
    rules: any[];
  }) {
    if (obj.combinator) {
      obj["condition"] = obj.combinator.toUpperCase();
      delete obj["combinator"];
    }

    if (Array.isArray(obj.rules)) {
      obj.rules.forEach((rule: any) => {
        processCombinators(rule);
      });
    }
    return obj;
  }

  const applyFilter = () => {
    let isTag;
    let isRelationship;
    let filtertype;

    let isFilterValidate = true;

    const highlightInvalidField = (ruleId: string) => {
      const element = document.querySelector(
        `[data-rule-id="${ruleId}"] [data-testid="value-editor"]`
      );
      if (element) {
        element.style.border = "2px solid red";
      }
    };

    const removeHighlightInvalidField = (ruleId: string) => {
      const element = document.querySelector(
        `[data-rule-id="${ruleId}"] [data-testid="value-editor"]`
      );
      if (element) {
        element.style.border = "";
      }
    };

    const validateFields = (query: RuleGroupType) => {
      const checkRules = (rules: any[]) => {
        let isValid = true;
        for (const rule of rules) {
          if (rule.operator !== "is_null" && rule.operator !== "not_null") {
            if (rule.rules) {
              if (!checkRules(rule.rules)) {
                isValid = false;
              }
            } else {
              if (!rule.field || !rule.operator || !rule.value) {
                highlightInvalidField(rule.id);
                isValid = false;
              } else {
                removeHighlightInvalidField(rule.id);
              }
            }
          }
        }
        return isValid;
      };

      return checkRules(query.rules);
    };

    if (
      !validateFields(typeQuery) ||
      !validateFields(classificationQuery) ||
      !validateFields(relationshipQuery)
    ) {
      return;
    }
    searchParams.set("pageLimit", 25);
    searchParams.set("pageOffset", 0);
    if (checkedEntities) {
      searchParams.set("includeDE", checkedEntities);
    } else {
      searchParams.delete("includeDE");
    }

    if (checkedSubClassifications) {
      searchParams.set("excludeSC", checkedSubClassifications);
    } else {
      searchParams.delete("excludeSC");
    }

    if (checkedSubTypes) {
      searchParams.set("excludeST", checkedSubTypes);
    } else {
      searchParams.delete("excludeST");
    }

    if (tagParams) {
      isTag = true;
      filtertype = isTag ? "tagFilters" : "entityFilters";
    }
    if (typeParams) {
      isTag = false;
      filtertype = isTag ? "tagFilters" : "entityFilters";
    }
    if (relationshipParams) {
      isTag = false;
      isRelationship = true;
      filtertype = "relationshipFilters";
    }

    if (!isEmpty(typeQuery)) {
      const fieldsArray = fields();
      const updatedRules = typeQuery.rules.map((rule) => {
        const fieldObj = fieldsArray.find((field) => field.name === rule.field);
        return {
          ...rule,
          type: fieldObj ? fieldObj.type : undefined
        };
      });
      const updatedTypeQuery = { ...typeQuery, rules: updatedRules };

      let queryBuilderData = cloneDeep(updatedTypeQuery);
      let ruleUrl;
      ruleUrl = attributeFilter.generateUrl({
        value: processCombinators(queryBuilderData),
        formatedDateToLong: true
      });

      if (!isEmpty(ruleUrl)) {
        searchParams.set("entityFilters", ruleUrl);
      } else {
        searchParams.delete("entityFilters");
      }
      navigate(
        {
          search: searchParams.toString()
        },
        { replace: true }
      );
    } else {
      isFilterValidate = false;
    }

    if (!isEmpty(classificationQuery)) {
      const fieldsArray = fields();
      const updatedRules = classificationQuery.rules.map((rule) => {
        const fieldObj = fieldsArray.find((field) => field.name === rule.field);
        return {
          ...rule,
          type: fieldObj ? fieldObj.type : undefined
        };
      });
      const updatedTypeQuery = { ...classificationQuery, rules: updatedRules };
      let queryBuilderData = cloneDeep(updatedTypeQuery);
      let ruleUrl;
      ruleUrl = attributeFilter.generateUrl({
        value: processCombinators(queryBuilderData),
        formatedDateToLong: true
      });

      if (!isEmpty(ruleUrl)) {
        searchParams.set("tagFilters", ruleUrl);
      } else {
        searchParams.delete("tagFilters");
      }
      navigate(
        {
          search: searchParams.toString()
        },
        { replace: true }
      );
    } else {
      isFilterValidate = false;
    }

    if (!isEmpty(entityFilters) || !isEmpty(typeQuery)) {
      globalSearchFilterInitialQuery.setQuery({ ["entityFilters"]: typeQuery });
    } else if (
      (isEmpty(entityFilters) && !isEmpty(tagFilters)) ||
      (isEmpty(entityFilters) && isEmpty(tagFilters))
    ) {
      globalSearchFilterInitialQuery.setQuery({ ["entityFilters"]: [] });
    }

    if (!isEmpty(tagFilters) || !isEmpty(classificationQuery)) {
      globalSearchFilterInitialQuery.setQuery({
        ["tagFilters"]: classificationQuery
      });
    } else if (
      (isEmpty(tagFilters) && !isEmpty(entityFilters)) ||
      (isEmpty(entityFilters) && isEmpty(tagFilters))
    ) {
      globalSearchFilterInitialQuery.setQuery({
        ["tagFilters"]: []
      });
    }

    setUpdateTable(moment.now());
    handleCloseFilterPopover();
  };

  return (
    <>
      <Popover
        id={popoverId}
        open={filtersOpen}
        anchorEl={filtersPopover}
        onClose={handleCloseFilterPopover}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "left"
        }}
        transformOrigin={{
          vertical: "top",
          horizontal: "left"
        }}
        sx={{
          "& .MuiPaper-root": {
            transitionDelay: "90ms !important"
          }
        }}
        PaperProps={{
          style: { margin: "0 auto" }
        }}
        disableScrollLock={true}
      >
        <div>
          <Stack width="700px" gap="1rem" margin="1rem">
            {isEmpty(relationshipParams) && (
              <Stack>
                <Accordion defaultExpanded>
                  <AccordionSummary
                    aria-controls="panel1-content"
                    id="panel1-header"
                  >
                    <Typography
                      className="text-color-green"
                      fontSize="16px"
                      fontWeight="600"
                    >
                      Include/Exclude
                    </Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    <Stack
                      direction="row"
                      justifyContent="flex-start"
                      alignItems="center"
                      gap="1rem"
                      marginBottom="0.75rem"
                    >
                      <Stack>
                        <FormGroup>
                          <FormControlLabel
                            control={
                              <AntSwitch
                                size="small"
                                checked={checkedEntities}
                                onChange={(
                                  e: React.ChangeEvent<HTMLInputElement>
                                ) => {
                                  handleSwitchChangeEntities(e);
                                }}
                                onClick={(e) => {
                                  e.stopPropagation();
                                }}
                                sx={{ marginRight: "4px" }}
                                inputProps={{ "aria-label": "controlled" }}
                              />
                            }
                            label="Show historical entities"
                          />
                        </FormGroup>
                      </Stack>
                      <Stack>
                        <FormGroup>
                          <FormControlLabel
                            control={
                              <AntSwitch
                                size="small"
                                checked={checkedSubClassifications}
                                sx={{ marginRight: "4px" }}
                                onChange={(
                                  e: React.ChangeEvent<HTMLInputElement>
                                ) => {
                                  handleSwitchChangeSubClassification(e);
                                }}
                                onClick={(e) => {
                                  e.stopPropagation();
                                }}
                                inputProps={{ "aria-label": "controlled" }}
                              />
                            }
                            label="Exclude sub-classifications"
                          />
                        </FormGroup>
                      </Stack>
                      <Stack>
                        <FormGroup>
                          <FormControlLabel
                            control={
                              <AntSwitch
                                size="small"
                                sx={{ marginRight: "4px" }}
                                checked={checkedSubTypes}
                                onChange={(
                                  e: React.ChangeEvent<HTMLInputElement>
                                ) => {
                                  handleSwitchChangeSubTypes(e);
                                }}
                                onClick={(e) => {
                                  e.stopPropagation();
                                }}
                                inputProps={{ "aria-label": "controlled" }}
                              />
                            }
                            label="Exclude sub-types"
                          />
                        </FormGroup>
                      </Stack>
                    </Stack>
                  </AccordionDetails>
                </Accordion>
              </Stack>
            )}
            <Stack gap="1rem">
              {!isEmpty(typeParams) && (
                <TypeFilters
                  allDataObj={allDataObj}
                  fieldsObj={fieldsObj}
                  typeQuery={typeQuery}
                  setTypeQuery={setTypeQuery}
                />
              )}
              {!isEmpty(tagParams) && (
                <TagFilters
                  allDataObj={allDataObj}
                  classificationQuery={classificationQuery}
                  setClassificationQuery={setClassificationQuery}
                />
              )}
              {!isEmpty(relationshipParams) && (
                <RelationshipFilters
                  allDataObj={allDataObj}
                  relationshipQuery={relationshipQuery}
                  setRelationshipQuery={setRelationshipQuery}
                />
              )}
            </Stack>
            <Stack
              direction="row"
              justifyContent={"flex-end"}
              padding="0 0.875rem"
              gap="0.5rem"
            >
              <Stack>
                <CustomButton
                  variant="contained"
                  size="small"
                  onClick={() => {
                    applyFilter();
                  }}
                >
                  Apply
                </CustomButton>
              </Stack>
              <Stack>
                <CustomButton
                  variant="outlined"
                  size="small"
                  onClick={() => {
                    handleCloseFilterPopover();
                  }}
                >
                  Close
                </CustomButton>
              </Stack>
            </Stack>
          </Stack>
        </div>
      </Popover>
    </>
  );
};

export default Filters;
