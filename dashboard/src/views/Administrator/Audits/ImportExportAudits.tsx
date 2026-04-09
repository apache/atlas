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

import { getValues } from "@components/commonComponents";
import { Typography } from "@components/muiComponents";
import { Divider, Stack } from "@mui/material";
import { category } from "@utils/Enum";
import { StyledPaper } from "@utils/Muiutils";
import { isArray, isEmpty, jsonParse } from "@utils/Utils";

const ImportExportAudits = ({ auditObj }: any) => {
  const { operation, params, result } = auditObj;
  const paramsObj = jsonParse(params);
  const resultObj = jsonParse(result);
  return (
    <>
      <Typography>{`${category[operation]}`}</Typography>
      <Stack>
        <StyledPaper variant="outlined">
          {" "}
          {!isEmpty(resultObj)
            ? Object.entries(resultObj)
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
                            flex: 1,
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
        </StyledPaper>

        <StyledPaper variant="outlined">
          {" "}
          {!isEmpty(paramsObj)
            ? Object.entries(paramsObj)
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
                            flex: 1,
                            wordBreak: "break-all",
                            textAlign: "left"
                          }}
                        >
                          {getValues(
                            paramsObj,
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
        </StyledPaper>

        <StyledPaper variant="outlined">
          {getValues(paramsObj, undefined, undefined, undefined, "properties")}
        </StyledPaper>
      </Stack>
    </>
  );
};
export default ImportExportAudits;
