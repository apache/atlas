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

import { Typography } from "@components/muiComponents";
import { useState } from "react";
import MuiLink from "@mui/material/Link";
import { isEmpty } from "@utils/Utils";

const ShowMoreText = ({
  value = "",
  maxLength = 160,
  more = "show more..",
  less = "show less..",
  isHtml = false
}: {
  value: string;
  maxLength: number;
  more: string;
  less: string;
  isHtml?: boolean;
}) => {
  const [showMore, setShowMore] = useState<boolean>(
    value.length > maxLength ? true : false
  );

  if (isEmpty(value)) return <>NA</>;

  const truncatedValue = value.substring(0, maxLength);

  return (
    <>
      {showMore && value.length > maxLength ? (
        <>
          {isHtml ? (
            <div
              className="long-descriptions"
              dangerouslySetInnerHTML={{ __html: truncatedValue }}
            />
          ) : (
            <>{truncatedValue}</>
          )}
          ...{" "}
          <MuiLink
            underline="hover"
            style={{ cursor: "pointer" }}
            onClick={(event) => {
              event.preventDefault();
              setShowMore(false);
            }}
            sx={{ display: "inline-block" }}
          >
            <Typography fontSize="14px" fontWeight={500}>
              {more}
            </Typography>
          </MuiLink>
        </>
      ) : (
        <>
          {isHtml ? (
            <div
              className="long-descriptions"
              dangerouslySetInnerHTML={{ __html: value }}
            />
          ) : (
            <>{value}</>
          )}
          {value.length > maxLength && (
            <MuiLink
              underline="hover"
              style={{ cursor: "pointer" }}
              onClick={() => {
                setShowMore(true);
              }}
              sx={{ display: "inline-block" }}
            >
              <Typography fontSize="14px" fontWeight={500}>
                {less}
              </Typography>
            </MuiLink>
          )}
        </>
      )}
    </>
  );
};

export default ShowMoreText;
