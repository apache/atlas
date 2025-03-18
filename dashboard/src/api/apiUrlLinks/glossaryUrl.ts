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

const glossaryUrl = () => {
  let termUrl = getBaseApiUrl("urlV2") + `/glossary`;
  return termUrl;
};

const removeTermUrl = (currentVal: string) => {
  let termUrl =
    getBaseApiUrl("urlV2") + `/glossary/terms/${currentVal}/assignedEntities`;
  return termUrl;
};

const glossaryImportTempUrl = () => {
  return `${glossaryUrl()}/import/template`;
};

const glossaryImportUrl = () => {
  return `${glossaryUrl()}/import`;
};

const glossaryTypeUrl = (glossaryType: string, guid: string) => {
  return `${glossaryUrl()}/${glossaryType}/${guid}`;
};

const editGlossaryUrl = (guid: string) => {
  return `${glossaryUrl()}/${guid}`;
};

const createTermorCategoryUrl = (type: string) => {
  return `${glossaryUrl()}/${type}`;
};

const editTermorCategoryUrl = (type: string, guid: string) => {
  return `${glossaryUrl()}/${type}/${guid}`;
};

const deleteGlossaryorTermUrl = (guid: string) => {
  let deleteTag = `${getBaseApiUrl("urlV2")}/glossary/${guid}`;
  return deleteTag;
};

const assignTermtoEntitiesUrl = (guid: string) => {
  let termUrl =
    getBaseApiUrl("urlV2") + `/glossary/terms/${guid}/assignedEntities`;
  return termUrl;
};

const assignTermtoCategoryUrl = (guid: string) => {
  let termUrl = getBaseApiUrl("urlV2") + `/glossary/category/${guid}`;
  return termUrl;
};

const assignGlossaryTypeUrl = (guid: string) => {
  let termUrl = getBaseApiUrl("urlV2") + `/glossary/term/${guid}`;
  return termUrl;
};

const removeTermorCatgeoryUrl = (guid: string, glossaryType: string) => {
  let termUrl = getBaseApiUrl("urlV2") + `/glossary/${glossaryType}/${guid}`;
  return termUrl;
};
export {
  removeTermUrl,
  glossaryUrl,
  glossaryImportTempUrl,
  glossaryImportUrl,
  glossaryTypeUrl,
  editGlossaryUrl,
  deleteGlossaryorTermUrl,
  createTermorCategoryUrl,
  editTermorCategoryUrl,
  assignTermtoEntitiesUrl,
  assignTermtoCategoryUrl,
  assignGlossaryTypeUrl,
  removeTermorCatgeoryUrl,
};
