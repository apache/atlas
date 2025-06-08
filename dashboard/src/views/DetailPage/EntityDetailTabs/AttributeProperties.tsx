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

import { Divider, Stack, Typography } from "@mui/material";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  CustomButton,
  LightTooltip
} from "@components/muiComponents";
import { isArray, isEmpty, isNull } from "@utils/Utils";
import SkeletonLoader from "@components/SkeletonLoader";
import { getValues } from "@components/commonComponents";
import { useState } from "react";
import { useSelector } from "react-redux";
import { EntityState } from "@models/relationshipSearchType";
import EntityForm from "@views/Entity/EntityForm";
import { useAppSelector } from "@hooks/reducerHook";
import { AntSwitch } from "@utils/Muiutils";
import { cloneDeep } from "@utils/Helper";

const AttributeProperties = ({
  entity,
  referredEntities,
  loading,
  auditDetails,
  entityobj,
  propertiesName
}: any) => {
  const {
    attributes = {},
    relationshipAttributes = {},
    typeName = "",
    customAttributes = {}
  } = entity || {};
  const { sessionObj = "" }: any = useAppSelector(
    (state: any) => state.session
  );
  const { entityData = {} } = useSelector((state: EntityState) => state.entity);
  const { entityDefs = [] } = entityData || {};
  const { data = {} } = sessionObj || {};

  const key = "atlas.entity.update.allowed";

  let entityUpdate: boolean = false;
  let entityTypeConfList = [];
  if (!isEmpty(data?.[key])) {
    let entityTypeList = data["atlas.ui.editable.entity.types"]
      .trim()
      .split(",");
    if (entityTypeList.length) {
      if (entityTypeList[0] === "*") {
        entityTypeConfList = [];
      } else if (entityTypeList.length > 0) {
        entityTypeConfList = entityTypeList;
      }
    }
  }

  if (entityTypeConfList && isEmpty(entityTypeConfList)) {
    entityUpdate = true;
  } else {
    if (entityTypeConfList.includes(typeName)) {
      entityUpdate = true;
    }
  }

  const [entityModal, setEntityModal] = useState<boolean>(false);
  const [checked, setChecked] = useState<boolean>(false);

  const handleCloseEntityModal = () => {
    setEntityModal(false);
  };
  const handleOpenEntityModal = () => {
    setEntityModal(true);
  };

  const getAttr = (propertiesName: string) => {
    if (propertiesName == "Technical") {
      return attributes;
    } else if (propertiesName == "Relationship") {
      return relationshipAttributes;
    } else if ((propertiesName = "User-defined")) {
      return customAttributes;
    }
  };

  let properties = !isEmpty(entity)
    ? { ...(getAttr(propertiesName) || {}) }
    : {};
  if (!isEmpty(properties) && !auditDetails) {
    properties["typeName"] = typeName;
  }
  let nonEmptyValueProperty: { [key: string]: string } = {};

  if (!isEmpty(properties) && !auditDetails) {
    let activeTypeDef = entityDefs.find((obj: { name: any }) => {
      return obj.name == entity.typeName;
    });
    let attributes: any[];
    const processSuperTypes = (superTypeName: string) => {
      let superTypesEntityDef = entityDefs.find((obj: { name: string }) => {
        return obj.name == superTypeName;
      });
      attributes = [...attributes, ...superTypesEntityDef.attributeDefs];

      if (superTypesEntityDef && superTypesEntityDef.superTypes) {
        for (let nestedSuperType of superTypesEntityDef.superTypes) {
          processSuperTypes(nestedSuperType);
        }
      }
    };

    attributes = activeTypeDef.attributes || [];
    for (let superType of activeTypeDef.superTypes) {
      attributes = [...attributes, ...activeTypeDef.attributeDefs];
      processSuperTypes(superType);
    }

    for (let property in properties) {
      let propertyType = attributes.find(
        (obj: { name: string }) => obj.name == property
      )?.typeName;
      if (propertyType == "date" && properties[property] == 0) {
        properties[property] = null;
      }
      if (!isEmpty(properties[property])) {
        nonEmptyValueProperty[property] = properties[property];
      }
    }
  }

  const handleSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    event.stopPropagation();
    setChecked(event.target.checked);
  };

  let filterEntityData = cloneDeep(entityData);
  let typeDefEntityData = !isNull(filterEntityData)
    ? filterEntityData.entityDefs.find((entitys: { name: string }) => {
        if (
          entitys.name ==
          (auditDetails ? entityobj?.typeName : properties?.typeName)
        ) {
          return entitys;
        }
      })
    : {};
  return (
    <>
      <Accordion defaultExpanded>
        <AccordionSummary
          aria-controls="technical-properties-content"
          id="technical-properties-header"
        >
          <Stack direction="row" alignItems="center" flex="1">
            <div className="properties-panel-name">
              <Typography fontWeight="600" className="text-color-green">
                {propertiesName} Properties
              </Typography>{" "}
            </div>

            {!auditDetails && (
              <Stack direction="row" alignItems="center" gap="0.5rem">
                {" "}
                <LightTooltip
                  title={checked ? "Hide empty values" : "Show empty values"}
                >
                  <AntSwitch
                    size="small"
                    checked={checked}
                    onChange={(e) => {
                      handleSwitchChange(e);
                    }}
                    onClick={(e) => {
                      e.stopPropagation();
                    }}
                    inputProps={{ "aria-label": "controlled" }}
                  />
                </LightTooltip>
                {entityUpdate && (
                  <LightTooltip title={"Edit Entity"}>
                    <CustomButton
                      variant="outlined"
                      size="small"
                      color="success"
                      onClick={(e: { stopPropagation: () => void }) => {
                        e.stopPropagation();
                        handleOpenEntityModal();
                      }}
                    >
                      Edit
                    </CustomButton>
                  </LightTooltip>
                )}{" "}
              </Stack>
            )}
          </Stack>
        </AccordionSummary>
        <AccordionDetails>
          {loading == undefined || loading ? (
            <>
              <SkeletonLoader count={3} animation="wave" />
            </>
          ) : (
            <Stack>
              {!isEmpty(
                !auditDetails
                  ? checked
                    ? properties
                    : nonEmptyValueProperty
                  : properties
              )
                ? Object.entries(
                    !auditDetails
                      ? checked
                        ? properties
                        : nonEmptyValueProperty
                      : properties
                  )
                    .sort()
                    .map(([keys, value]: [string, any]) => {
                      return (
                        <>
                          <Stack
                            direction="row"
                            spacing={4}
                            marginBottom={1}
                            marginTop={1}
                          >
                            <div
                              style={{
                                flexBasis: "30%",
                                wordBreak: "break-all",
                                textAlign: "left",
                                fontWeight: "600"
                              }}
                            >
                              {`${keys} ${
                                isArray(value) ? `(${value.length})` : ""
                              }`}
                            </div>
                            <div
                              style={{
                                flex: 1,
                                wordBreak: "break-all",
                                textAlign: "left",
                                margin: "0"
                              }}
                              className="attribute-value-text"
                            >
                              {getValues(
                                value,
                                properties,
                                typeDefEntityData,
                                undefined,
                                "properties",
                                referredEntities,
                                entity,
                                keys
                              )}
                            </div>
                          </Stack>
                          <Divider />
                        </>
                      );
                    })
                : "No Record Found"}
            </Stack>
          )}
        </AccordionDetails>
      </Accordion>
      {entityModal && (
        <EntityForm open={entityModal} onClose={handleCloseEntityModal} />
      )}
    </>
  );
};

export default AttributeProperties;
