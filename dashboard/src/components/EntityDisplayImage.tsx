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

import { useState, useEffect, useRef, useLayoutEffect } from "react";
import { Avatar } from "@mui/material";
import { getEntityIconPath } from "../utils/Utils";

interface EntityData {
  guid?: string | number;
  isProcess?: boolean;
  [key: string]: unknown;
}

interface DisplayImageProps {
  entity: EntityData;
  width?: string | number;
  height?: string | number;
  avatarDisplay?: boolean;
  isProcess?: boolean;
}

const toCssSize = (value: string | number | undefined): string | undefined => {
  if (value === undefined) {
    return undefined;
  }
  return typeof value === "number" ? `${value}px` : value;
};

const DisplayImage = ({
  entity,
  width,
  height,
  avatarDisplay,
  isProcess,
}: DisplayImageProps) => {
  const entityData = { ...entity, isProcess };

  const primaryUrl = getEntityIconPath({ entityData }) || "";
  const fallbackUrl =
    getEntityIconPath({ entityData, errorUrl: primaryUrl }) || "";

  const [imgSrc, setImgSrc] = useState(primaryUrl);
  const avatarWrapperRef = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    setImgSrc(primaryUrl);
  }, [primaryUrl]);

  useLayoutEffect(() => {
    const el = avatarWrapperRef.current;
    if (!el) {
      return;
    }
    const cssWidth = toCssSize(width);
    const cssHeight = toCssSize(height);
    if (cssWidth) {
      el.style.setProperty("--entity-avatar-width", cssWidth);
    }
    if (cssHeight) {
      el.style.setProperty("--entity-avatar-height", cssHeight);
    }
  }, [width, height]);

  const handleError = () => {
    if (imgSrc !== fallbackUrl) {
      setImgSrc(fallbackUrl);
    }
  };

  return (
    <div className="search-result-table-name-col" data-cy="entityIcon">
      {avatarDisplay === undefined ? (
        <img
          className="search-result-table-img"
          id={entity.guid ? String(entity.guid) : undefined}
          data-cy={entity.guid ? String(entity.guid) : undefined}
          src={imgSrc}
          alt="Entity Icon"
          onError={handleError}
        />
      ) : (
        <span ref={avatarWrapperRef} className="entity-display-avatar-wrapper">
          <Avatar
            alt="entityImg"
            src={imgSrc}
            className="entity-display-avatar"
            variant="square"
            imgProps={{ onError: handleError }}
          />
        </span>
      )}
    </div>
  );
};

export default DisplayImage;
