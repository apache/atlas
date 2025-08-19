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
  Divider,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableRow,
  Typography
} from "@mui/material";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary
} from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import { isObject } from "@utils/Utils";
import { customSortObj, numberFormatWithBytes } from "@utils/Helper";
import { getValues } from "@components/commonComponents";

const SystemDetails = () => {
  const { metricsData }: any = useAppSelector((state: any) => state.metrics);
  const { system } = metricsData?.data || {};
  let systemData = system;
  let systemOS = systemData?.os || {};
  let systemRuntimeData = systemData?.runtime || {};
  let systemMemoryData: any = customSortObj(systemData?.memory) || {};
  return (
    <Accordion>
      <AccordionSummary
        aria-controls="technical-properties-content"
        id="technical-properties-header"
      >
        <Stack direction="row" alignItems="center" flex="1">
          <div className="properties-panel-name">
            <Typography fontWeight="600" className="text-color-green">
              System Details
            </Typography>
          </div>
        </Stack>
      </AccordionSummary>
      <AccordionDetails>
        <Stack gap="1rem">
          <Stack direction="row" gap="1rem">
            <Stack width="50%">
              <Paper elevation={3} style={{ padding: "16px" }}>
                <Typography fontWeight="600">OS</Typography>
                <Divider />
                <Table>
                  <TableBody>
                    {Object.keys(systemOS).map((key, index) => (
                      <TableRow key={index}>
                        <TableCell component="th" scope="row">
                          {key}
                        </TableCell>

                        <TableCell align="right">{systemOS[key]}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </Paper>
            </Stack>
            <Stack width="50%">
              {" "}
              <Paper elevation={3} style={{ padding: "16px" }}>
                <Typography fontWeight="600">Runtime</Typography>
                <Divider />
                <Table>
                  <TableBody>
                    {Object.keys(systemRuntimeData).map((key, index) => (
                      <TableRow key={index}>
                        <TableCell component="th" scope="row">
                          {key}
                        </TableCell>

                        <TableCell align="right">
                          {systemRuntimeData[key]}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </Paper>
            </Stack>
          </Stack>
          <Stack>
            {" "}
            <Paper elevation={3} style={{ padding: "16px" }}>
              <Typography fontWeight="600">Memory</Typography>
              <Divider />
              <Table>
                <TableBody>
                  {Object.keys(systemMemoryData).map((key, index) => (
                    <TableRow key={index}>
                      <TableCell component="th" scope="row">
                        {key}
                      </TableCell>

                      <TableCell align="right">
                        {!isObject(systemMemoryData[key])
                          ? numberFormatWithBytes(systemMemoryData[key])
                          : getValues(
                              systemMemoryData[key],
                              undefined,
                              undefined,
                              undefined,
                              "properties"
                            )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </Paper>
          </Stack>
        </Stack>
      </AccordionDetails>
    </Accordion>
  );
};

export default SystemDetails;
