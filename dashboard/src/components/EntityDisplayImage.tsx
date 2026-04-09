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

import { useEffect, useState } from "react";
import { Avatar, Skeleton } from "@mui/material";
import { getEntityIconPath } from "../utils/Utils";

const DisplayImage = ({
  entity,
  width,
  height,
  avatarDisplay,
  isProcess
}: any) => {
  const [imageUrl, setImageUrl] = useState<any>(null);
  const [checkEntityImage, setCheckEntityImage] = useState<any>({
    [entity.guid]: false
  });

  useEffect(() => {
    const fetchImagePath = async () => {
      let entityData = { ...entity, ...{ isProcess: isProcess } };
      let imagePath: any = getEntityIconPath({ entityData: entityData });
      try {
        const response = await fetch(imagePath);
        const contentType: any = response.headers.get("Content-Type");

        if (contentType.startsWith("image/")) {
          let cache = { [entityData.guid]: imagePath };
          setCheckEntityImage(cache);
          setImageUrl(getEntityIconPath({ entityData: entityData }));
        } else {
          setImageUrl(
            getEntityIconPath({ entityData: entityData, errorUrl: imagePath })
          );
        }
      } catch (error) {
        setImageUrl(
          getEntityIconPath({ entityData: entityData, errorUrl: imagePath })
        );
      }
    };

    fetchImagePath();
  }, []);

  return imageUrl != undefined ? (
    <div className="search-result-table-name-col" data-cy="entityIcon">
      {checkEntityImage[entity.guid] !== false ? (
        avatarDisplay == undefined ? (
          <img
            className="search-result-table-img"
            id={entity.guid}
            data-cy={entity.guid}
            src={checkEntityImage[entity.guid]}
            alt="Entity Icon"
          />
        ) : (
          <Avatar
            alt="entityImg"
            src={checkEntityImage[entity.guid]}
            sx={{ width: width, height: height }}
            variant="square"
          ></Avatar>
        )
      ) : avatarDisplay == undefined ? (
        <img
          className="search-result-table-img"
          id={entity.guid}
          data-cy={entity.guid}
          src={imageUrl}
          alt="Entity Icon"
        />
      ) : (
        <Avatar
          alt="entityImg"
          src={imageUrl}
          sx={{ width: width, height: height }}
        ></Avatar>
      )}
    </div>
  ) : (
    <div>{<Skeleton variant="circular" width={22} height={20} />}</div>
  );
};

export default DisplayImage;
