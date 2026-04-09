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

import { getBaseApiUrl } from "./commonApiUrl";

const removeClassificationUrl = (obj: string, currentVal: string) => {
  let classificationUrl =
    getBaseApiUrl("urlV2") + `/entity/guid/${obj}/classification/${currentVal}`;
  return classificationUrl;
};

const addTagUrl = () => {
  let tagUrl = `${getBaseApiUrl("urlV2")}/entity/bulk/classification`;
  return tagUrl;
};

const deleteTagUrl = (tagName: string) => {
  let deleteTag = `${getBaseApiUrl("urlV2")}/types/typedef/name/${tagName}`;
  return deleteTag;
};

const editAssignTagUrl = (guid: string) => {
  let editTagUrl =
    getBaseApiUrl("urlV2") + `/entity/guid/${guid}/classifications`;
  return editTagUrl;
};

const rootClassificationDefUrl = (name: string) => {
  let allTagUrl = `${getBaseApiUrl(
    "urlV2"
  )}/types/classificationdef/name/${name}`;
  return allTagUrl;
};

export {
  removeClassificationUrl,
  addTagUrl,
  deleteTagUrl,
  editAssignTagUrl,
  rootClassificationDefUrl
};
