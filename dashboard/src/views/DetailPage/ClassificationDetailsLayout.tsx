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

import { Stack } from "@mui/material";
import DetailPageAttribute from "./DetailPageAttributes";
import SearchResult from "@views/SearchResult/SearchResult";
import { useParams } from "react-router-dom";
import { paramsType, tagNameType } from "@models/detailPageType";
import { useAppSelector } from "@hooks/reducerHook";
import { isEmpty } from "@utils/Utils";
import { cloneDeep } from "@utils/Helper";

const ClassificationDetailsLayout = () => {
  const { tagName } = useParams<paramsType>();

  const { classificationData = {}, loading }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { classificationDefs = {} } = classificationData || {};
  let classificationDefObj = cloneDeep(classificationDefs);

  let tag = !isEmpty(classificationDefObj)
    ? classificationDefObj.find((obj: tagNameType) => obj.name == tagName)
    : {};

  const {
    subTypes = {},
    superTypes = {},
    entityTypes = {},
    attributeDefs = {},
    description = {}
  } = tag || {};
  return (
    <Stack direction="column" gap="1rem">
      <DetailPageAttribute
        paramsAttribute={tagName}
        data={tag}
        description={description}
        subTypes={subTypes}
        superTypes={superTypes}
        entityTypes={entityTypes}
        loading={loading}
        attributeDefs={attributeDefs}
      />
      <SearchResult classificationParams={tagName} />
    </Stack>
  );
};

export default ClassificationDetailsLayout;
