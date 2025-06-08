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

import { Divider, Grid, Stack, Typography } from "@mui/material";
import { isArray, isEmpty, isNull } from "@utils/Utils";

import { useSelector } from "react-redux";
import { EntityState } from "@models/relationshipSearchType";
import { getValues } from "@components/commonComponents";

const RauditsTableResults = ({ componentProps, row }: any) => {
  const { entity, referredEntities } = componentProps;
  let detailObj = row.original;
  let parseDetailsObject: unknown;

  const { entityData } = useSelector((state: EntityState) => state.entity);

  let filterEntityData = entityData;
  let typeDefEntityData = !isNull(filterEntityData)
    ? filterEntityData.entityDefs.find((entitys: { name: string }) => {
        if (entitys.name == entity.typeName) {
          return entitys;
        }
      })
    : {};

  const getAuditDetails = (detailObj: any) => {
    try {
      parseDetailsObject = JSON.parse(detailObj.resultSummary);

      if (parseDetailsObject) {
        return (
          <Grid
            container
            marginTop={0}
            spacing={2}
            className="properties-container"
          >
            <Grid item md={12} paddingTop={"0 !important"}>
              {!isEmpty(parseDetailsObject) && (
                <Typography>
                  <Stack border="1px solid #dddddd" bgcolor="white">
                    {!isEmpty(parseDetailsObject)
                      ? Object.entries(parseDetailsObject)
                          .sort()
                          .map(([keys, value]: [string, any]) => {
                            return (
                              <>
                                <Stack direction="row" spacing={4} margin={1}>
                                  <div
                                    style={{
                                      flex: 1,
                                      wordBreak: "break-all",
                                      textAlign: "left",
                                      fontWeight: "600",
                                      maxWidth: "320px"
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
                                      textAlign: "left"
                                    }}
                                  >
                                    {getValues(
                                      value,
                                      typeDefEntityData,
                                      entity,
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
                </Typography>
              )}
            </Grid>
          </Grid>
        );
      }
    } catch (error) {
      return (
        <h4 data-cy="noData">
          <i>No details to show!</i>
        </h4>
      );
    }
  };

  return getAuditDetails(detailObj);
};

export default RauditsTableResults;
