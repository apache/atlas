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

import SkeletonLoader from "@components/SkeletonLoader";
import { Divider, Stack, Typography } from "@mui/material";
import { stats } from "@utils/Enum";
import { dateFormat, isArray, isEmpty } from "@utils/Utils";
import moment from "moment";

const TermProperties = ({ additionalAttributes, loader }: any) => {
  const getValue = (values: any, type: string) => {
    if (type == "time") {
      return moment().milliseconds(values);
    } else if (type == "day") {
      return dateFormat(values);
    } else {
      return values;
    }
  };

  return loader ? (
    <>
      <SkeletonLoader count={3} animation="wave" />
    </>
  ) : (
    <Stack padding={2}>
      <Typography className="text-color-green term-properties">
        Additional Properties:
      </Typography>
      <Divider />
      {!isEmpty(additionalAttributes)
        ? Object.entries(additionalAttributes).map(
            ([keys, value]: [string, any]) => {
              return (
                <>
                  <Stack
                    direction="row"
                    spacing={4}
                    marginBottom={1}
                    marginTop={1}
                    data-cy="properties-card"
                  >
                    <div
                      style={{
                        flex: 1,
                        wordBreak: "break-all",
                        textAlign: "left",
                        fontWeight: "600",
                      }}
                    >
                      {`${keys} ${isArray(value) ? `(${value.length})` : ""}`}
                    </div>
                    <div
                      style={{
                        flex: 1,
                        wordBreak: "break-all",
                        textAlign: "left",
                      }}
                    >
                      {getValue(value, stats[keys])}
                    </div>
                  </Stack>
                  <Divider />
                </>
              );
            }
          )
        : "No Record Found"}
    </Stack>
  );
};
export default TermProperties;
