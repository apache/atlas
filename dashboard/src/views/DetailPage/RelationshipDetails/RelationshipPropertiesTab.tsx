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

import { getValues } from "@components/commonComponents";
import SkeletonLoader from "@components/SkeletonLoader";
import { isArray, isEmpty, pick } from "@utils/Utils";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary
} from "@components/muiComponents";

const RelationshipPropertiesTab = (props: {
  entity: any;
  loading: boolean;
}) => {
  const { entity, loading } = props;

  let relationshipObj = pick(entity, [
    "createTime",
    "createdBy",
    "blockedPropagatedClassifications",
    "guid",
    "label",
    "propagateTags",
    "propagatedClassifications",
    "provenanceType",
    "status",
    "updateTime",
    "updatedBy",
    "version"
  ]);

  let nonEmptyValueProperty: { [key: string]: string } = {};

  if (!isEmpty(relationshipObj)) {
    for (let property in relationshipObj) {
      if (!isEmpty(relationshipObj[property])) {
        nonEmptyValueProperty[property] = relationshipObj[property];
      }
    }
  }

  const { end1, end2 } = entity;

  return (
    <Grid
      container
      marginTop={0}
      className="properties-container"
      sx={{ backgroundColor: "rgba(255,255,255,0.6)" }}
      flex="1"
    >
      <Grid item md={6} p={2} data-cy="technical-properties">
        <Accordion defaultExpanded>
          <AccordionSummary
            aria-controls="technical-properties-content"
            id="technical-properties-header"
          >
            <Stack direction="row" alignItems="center" flex="1">
              <div className="properties-panel-name">
                <Typography fontWeight="600" className="text-color-green">
                  Technical Properties
                </Typography>
              </div>
            </Stack>
          </AccordionSummary>
          <AccordionDetails>
            {loading == undefined || loading ? (
              <>
                <SkeletonLoader count={3} animation="wave" />
              </>
            ) : (
              <Typography>
                <Stack>
                  {!isEmpty(nonEmptyValueProperty)
                    ? Object.entries(nonEmptyValueProperty)
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
                                >
                                  {getValues(
                                    value,
                                    undefined,
                                    undefined,
                                    undefined,
                                    "properties",
                                    undefined,
                                    value,
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
          </AccordionDetails>
        </Accordion>{" "}
      </Grid>
      <Grid item md={6} p={2}>
        <Stack gap={"1rem"}>
          {/* Relationship Properties */}
          <Accordion defaultExpanded>
            <AccordionSummary
              aria-controls="technical-properties-content"
              id="technical-properties-header"
            >
              <Stack direction="row" alignItems="center" flex="1">
                <div className="properties-panel-name">
                  <Typography fontWeight="600" className="text-color-green">
                    Relationship Properties
                  </Typography>
                </div>
              </Stack>
            </AccordionSummary>
            <AccordionDetails>No Record found!</AccordionDetails>
          </Accordion>

          {/* End1 */}
          <Accordion defaultExpanded={false}>
            <AccordionSummary
              aria-controls="technical-properties-content"
              id="technical-properties-header"
            >
              <Stack direction="row" alignItems="center" flex="1">
                <div className="properties-panel-name">
                  <Typography fontWeight="600" className="text-color-green">
                    End1
                  </Typography>
                </div>
              </Stack>
            </AccordionSummary>
            <AccordionDetails>
              {" "}
              {!isEmpty(end1)
                ? Object.entries(end1)
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
                                flex: 1,
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
                                // flex: 1,
                                wordBreak: "break-all",
                                textAlign: "left"
                              }}
                            >
                              {getValues(
                                value,
                                undefined,
                                undefined,
                                undefined,
                                "properties"
                              )}
                            </div>
                          </Stack>
                          <Divider />
                        </>
                      );
                    })
                : "No Record Found"}
            </AccordionDetails>
          </Accordion>
          {/* End2 */}
          <Accordion defaultExpanded={false}>
            <AccordionSummary
              aria-controls="technical-properties-content"
              id="technical-properties-header"
            >
              <Stack direction="row" alignItems="center" flex="1">
                <div className="properties-panel-name">
                  <Typography fontWeight="600" className="text-color-green">
                    End2
                  </Typography>
                </div>
              </Stack>
            </AccordionSummary>
            <AccordionDetails>
              {" "}
              {!isEmpty(end1)
                ? Object.entries(end2)
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
                                flex: 1,
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
                                // flex: 1,
                                wordBreak: "break-all",
                                textAlign: "left"
                              }}
                            >
                              {getValues(
                                value,
                                undefined,
                                undefined,
                                undefined,
                                "properties"
                              )}
                            </div>
                          </Stack>
                          <Divider />
                        </>
                      );
                    })
                : "No Record Found"}
            </AccordionDetails>{" "}
          </Accordion>
        </Stack>
      </Grid>
    </Grid>
  );
};

export default RelationshipPropertiesTab;
