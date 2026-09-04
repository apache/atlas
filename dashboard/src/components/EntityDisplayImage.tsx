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

import type { SyntheticEvent } from "react";
import { Avatar } from "@mui/material";
import { getEntityIconPath } from "../utils/Utils";

interface DisplayImageProps {
  entity: Record<string, unknown>;
  width?: string | number;
  height?: string | number;
  avatarDisplay?: boolean;
  isProcess?: boolean;
}

const DisplayImage = ({
  entity,
  width,
  height,
  avatarDisplay,
  isProcess
}: DisplayImageProps) => {
  const entityData = { ...entity, isProcess };
  
  const primaryUrl = getEntityIconPath({ entityData }) || "";
  const fallbackUrl = getEntityIconPath({ entityData, errorUrl: primaryUrl }) || "";

  const handleError = (e: SyntheticEvent<HTMLImageElement, Event>) => {
    const target = e.currentTarget;
    if (target.dataset.fallbackApplied !== "true") {
      target.dataset.fallbackApplied = "true";
      target.onerror = null;
      target.src = fallbackUrl;
    }
  };

  return (
    <div className="search-result-table-name-col" data-cy="entityIcon">
      {avatarDisplay === undefined ? (
        <img
          className="search-result-table-img"
          id={entity.guid ? String(entity.guid) : undefined}
          data-cy={entity.guid ? String(entity.guid) : undefined}
          src={primaryUrl}
          alt="Entity Icon"
          onError={handleError}
        />
      ) : (
        <Avatar
          alt="entityImg"
          src={primaryUrl}
          sx={{ width: width, height: height }}
          variant="square"
          imgProps={{ onError: handleError }}
        ></Avatar>
      )}
    </div>
  );
};

export default DisplayImage;
