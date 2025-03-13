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
  Stack,
  Typography,
  Table,
  TableBody,
  TableRow,
  TableCell
} from "@mui/material";
import moment from "moment";

const GraphCustomTooltip = ({ active, payload, label, coordinate }: any) => {
  if (active && payload?.length) {
    const total = payload.reduce(
      (sum: number, entry: any) => sum + entry.value,
      0
    );

    return (
      <Stack
        sx={{
          transform: `translate(${coordinate.x}px, ${coordinate.y}px)`
        }}
        className="graph-custom-tooltip"
        data-cy="graph-custom-tooltip"
      >
        <Typography variant="subtitle2" className="graph-custom-tooltip-title">
          {moment(label).format("MM/DD/YYYY")}
        </Typography>

        <Table size="small">
          <TableBody>
            {payload.map((entry: any, index: number) => (
              <TableRow key={index}>
                <TableCell className="graph-custom-tooltip-icon">
                  <Stack
                    sx={{
                      width: "10px",
                      height: "10px",
                      backgroundColor: entry.color,
                      borderRadius: "50%"
                    }}
                  />
                </TableCell>
                <TableCell
                  sx={{
                    padding: "4px",
                    borderBottom: "none",
                    color: "rgba(0, 0, 0, 0.87)"
                  }}
                >
                  {entry.name}
                </TableCell>
                <TableCell
                  sx={{
                    padding: "4px",
                    borderBottom: "none",
                    textAlign: "right",
                    color: "rgba(0, 0, 0, 0.87)"
                  }}
                >
                  {entry.value.toFixed(2)}
                </TableCell>
              </TableRow>
            ))}
            <TableRow>
              <TableCell
                sx={{
                  padding: "4px",
                  borderBottom: "none"
                }}
              />

              <TableCell className="graph-custom-tooltip-total">
                TOTAL
              </TableCell>
              <TableCell className="graph-custom-tooltip-total-value">
                {total.toFixed(2)}
              </TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </Stack>
    );
  }

  return null;
};

export default GraphCustomTooltip;
