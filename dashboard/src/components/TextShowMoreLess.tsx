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

import Chip from "@mui/material/Chip";
import { useState } from "react";
import { EllipsisText } from "./commonComponents";
import { LightTooltip } from "./muiComponents";

export const TextShowMoreLess = (props: {
  data: string[];
  displayText?: string | undefined;
}) => {
  const [data, setData] = useState<any>(
    props?.data?.length > 2 ? props?.data?.slice(0, 1) : props?.data
  );
  const [show, setShow] = useState<boolean>(true);

  const handleShowMoreClick = () => {
    let showData: boolean = !show;
    let data = showData ? props?.data?.slice(0, 1) : props?.data;

    setShow(showData);
    setData(data);
  };

  return (
    <>
      <div
        className={show ? "show-less" : "show-more"}
        style={{ display: "flex", gap: "2px" }}
      >
        {data?.map((key: any) => {
          return (
            <LightTooltip
              title={
                props.displayText != undefined ? key[props.displayText] : key
              }
            >
              <Chip
                label={
                  <EllipsisText>
                    {props.displayText != undefined
                      ? key[props.displayText]
                      : key}
                  </EllipsisText>
                }
                variant="outlined"
                size="small"
                color="primary"
                className="chip-items"
                clickable
              />
            </LightTooltip>
          );
        })}
        <a onClick={handleShowMoreClick}>
          {props?.data?.length > 2 ? (
            show ? (
              <code
                className="show-more-less text-color-green"
                data-id="showMore"
                data-cy="showMore"
              >
                {" "}
                + More..
              </code>
            ) : (
              <code
                className="show-more-less text-color-green"
                data-id="showLess"
                data-cy="showLess"
              >
                {" "}
                - Less..
              </code>
            )
          ) : null}
        </a>
      </div>
    </>
  );
};
