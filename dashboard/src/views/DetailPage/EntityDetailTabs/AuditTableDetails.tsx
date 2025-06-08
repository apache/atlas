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

import { Grid } from "@mui/material";
import { extractKeyValueFromEntity, isArray, isEmpty } from "@utils/Utils";
import AttributeProperties from "./AttributeProperties";

const updateName = (entityName: string, entity: object) => {
  const { name }: { name: string; found: boolean; key: any } =
    extractKeyValueFromEntity(entity);
  return (
    <>
      <span className="audit-detail-name">
        Name: {!isEmpty(entity) ? `${name} (${entityName})` : entityName}
      </span>
    </>
  );
};

const AuditTableDetails = ({ componentProps, row }: any) => {
  const { entity, referredEntities, loading } = componentProps;
  let detailObj = row.original.details;
  let parseDetailsObject;
  let type: string = "";
  let auditData: string = "";

  const getAuditDetails = (detailObj: any) => {
    if (detailObj.search(":") >= 0) {
      (parseDetailsObject = detailObj.split(":")),
        (type = ""),
        (auditData = "");
      if (parseDetailsObject.length > 1) {
        type = parseDetailsObject[0];
        parseDetailsObject.shift();
        auditData = parseDetailsObject.join(":");
      }
      if (auditData.search("{") === -1) {
        if (
          type.trim() === "Added labels" ||
          type.trim() === "Deleted labels"
        ) {
          return updateName(auditData.trim().split(" ").join(","), {});
        } else {
          return updateName(auditData, {});
        }
      } else {
        try {
          parseDetailsObject = JSON.parse(auditData);
          var skipAttribute = parseDetailsObject.typeName ? "guid" : null;
          const { name }: { name: string; found: boolean; key: any } =
            extractKeyValueFromEntity(parseDetailsObject, null, skipAttribute);

          if (parseDetailsObject) {
            let relationshipAttributes =
              parseDetailsObject.relationshipAttributes;
            let customAttr = parseDetailsObject.customAttributes;
            return (
              <>
                {name == "-"
                  ? parseDetailsObject.typeName
                  : updateName(name, {})}
                {!isEmpty(entity) ? (
                  <Grid
                    container
                    marginTop={0}
                    spacing={2}
                    className="properties-container"
                  >
                    <Grid item md={6}>
                      {" "}
                      <AttributeProperties
                        entity={parseDetailsObject}
                        referredEntities={referredEntities}
                        loading={loading}
                        auditDetails={true}
                        entityobj={entity}
                        propertiesName="Technical"
                      />
                    </Grid>
                    {!isEmpty(relationshipAttributes) && (
                      <Grid item md={6}>
                        {" "}
                        <AttributeProperties
                          entity={parseDetailsObject}
                          referredEntities={referredEntities}
                          loading={loading}
                          auditDetails={true}
                          entityobj={entity}
                          propertiesName="Relationship"
                        />
                      </Grid>
                    )}
                    {!isEmpty(customAttr) && (
                      <Grid item md={12}>
                        {" "}
                        <AttributeProperties
                          entity={parseDetailsObject}
                          referredEntities={referredEntities}
                          loading={loading}
                          auditDetails={true}
                          entityobj={entity}
                          propertiesName="User-defined"
                        />
                      </Grid>
                    )}
                  </Grid>
                ) : (
                  <h4 data-cy="noData">
                    <i>No details to show!</i>
                  </h4>
                )}
              </>
            );
          } else {
            <h4 data-cy="noData">
              <i>No details to show!</i>
            </h4>;
          }
        } catch (error) {
          isArray(parseDetailsObject) && updateName(parseDetailsObject[0], {});
          return (
            <h4 data-cy="noData">
              <i>No details to show!</i>
            </h4>
          );
        }
      }
    } else if (detailObj == "Deleted entity" || detailObj == "Purged entity") {
      return !isEmpty(entity?.typeName) ? (
        updateName(entity.typeName, entity)
      ) : (
        <h4 data-cy="noData">
          <i>No details to show!</i>
        </h4>
      );
    }
  };

  return getAuditDetails(detailObj);
};

export default AuditTableDetails;
