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
import { isEmpty, sanitizeHtmlContent } from "@utils/Utils";

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
  const [isTruncated, setIsTruncated] = useState(true);

  if (isEmpty(value)) return <>NA</>;

  const shouldTruncate = value.length > maxLength && isTruncated;
  const displayValue = shouldTruncate
    ? value.substring(0, maxLength) + "..."
    : value;
  const safeValue = isHtml ? sanitizeHtmlContent(displayValue) : displayValue;

  return (
    <>
      {isHtml ? (
        <div
          className="long-descriptions"
          dangerouslySetInnerHTML={{ __html: safeValue }}
        />
      ) : (
        displayValue
      )}
      {value.length > maxLength && (
        <MuiLink
          underline="hover"
          onClick={() => setIsTruncated((prev) => !prev)}
          className="show-more-less-link"
        >
          <Typography fontSize="14px" fontWeight={500}>
            {shouldTruncate ? ` ${more}` : ` ${less}`}
          </Typography>
        </MuiLink>
      )}
    </>
  );
};

export default ShowMoreText;
