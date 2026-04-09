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

import {
  assignGlossaryTypeUrl,
  assignTermtoCategoryUrl,
  assignTermtoEntitiesUrl,
  createTermorCategoryUrl,
  deleteGlossaryorTermUrl,
  editGlossaryUrl,
  editTermorCategoryUrl,
  glossaryImportTempUrl,
  glossaryImportUrl,
  glossaryTypeUrl,
  glossaryUrl,
  removeTermorCatgeoryUrl,
  removeTermUrl
} from "../apiUrlLinks/glossaryUrl";
import { _delete, _get, _post, _put } from "./apiMethod";

interface configs {
  method: string;
  params: object;
}

interface importTmplConfigs {
  method: string;
  params: object;
}

interface importConfigs {
  method: string;
  data: object;
  params: object;
}

const removeTerm = (
  termId: string,
  data: { guid: string; relationshipGuid: string }
) => {
  return _put(removeTermUrl(termId), {
    method: "PUT",
    params: {},
    data: [data]
  });
};

const getGlossary = () => {
  const config: configs = {
    method: "GET",
    params: {}
  };
  return _get(glossaryUrl(), config);
};

const getGlossaryImportTmpl = (params: object) => {
  const config: importTmplConfigs = {
    method: "GET",
    params: params
  };
  return _get(glossaryImportTempUrl(), config);
};

const getGlossaryImport = (params: object, uploadProgress: any) => {
  const config: importConfigs = {
    method: "POST",
    params: {},
    data: params,
    ...uploadProgress
  };
  return _get(glossaryImportUrl(), config);
};

const getGlossaryType = (glossaryType: string, guid: string) => {
  const config: configs = {
    method: "GET",
    params: {}
  };
  return _get(glossaryTypeUrl(glossaryType, guid), config);
};

const createGlossary = (params: any) => {
  const config: importConfigs = {
    method: "POST",
    params: {},
    data: params
  };
  return _post(glossaryUrl(), config);
};

const createTermorCategory = (type: string, params: any) => {
  const config: importConfigs = {
    method: "POST",
    params: {},
    data: params
  };
  return _post(createTermorCategoryUrl(type), config);
};

const editGlossary = (guid: string, params: any) => {
  const config: importConfigs = {
    method: "PUT",
    params: {},
    data: params
  };
  return _put(editGlossaryUrl(guid), config);
};

const editTermorCatgeory = (type: string, guid: string, params: any) => {
  const config: importConfigs = {
    method: "PUT",
    params: {},
    data: params
  };
  return _put(editTermorCategoryUrl(type, guid), config);
};

const deleteGlossaryorTerm = (guid: string) => {
  return _delete(deleteGlossaryorTermUrl(guid), {
    method: "DELETE",
    params: {}
  });
};

const deleteGlossaryorType = (guid: string) => {
  return _delete(assignGlossaryTypeUrl(guid), {
    method: "DELETE",
    params: {}
  });
};
const assignTermstoEntites = (
  termId: string,
  data: { guid: string; relationshipGuid: string }
) => {
  return _put(assignTermtoEntitiesUrl(termId), {
    method: "POST",
    params: {},
    data: data
  });
};

const assignTermstoCategory = (categoryId: string, data: any) => {
  return _put(assignTermtoCategoryUrl(categoryId), {
    method: "PUT",
    params: {},
    data: data
  });
};

const assignGlossaryType = (glossaryTypeGuid: string, data: any) => {
  return _put(assignGlossaryTypeUrl(glossaryTypeGuid), {
    method: "PUT",
    params: {},
    data: data
  });
};
const removeTermorCategory = (
  guid: string,
  glossrayType: string,
  params: any
) => {
  return _delete(removeTermorCatgeoryUrl(guid, glossrayType), {
    method: "PUT",
    params: {},
    data: params
  });
};
export {
  removeTerm,
  getGlossary,
  getGlossaryImportTmpl,
  getGlossaryImport,
  getGlossaryType,
  createGlossary,
  editGlossary,
  deleteGlossaryorTerm,
  createTermorCategory,
  editTermorCatgeory,
  assignTermstoEntites,
  assignTermstoCategory,
  assignGlossaryType,
  removeTermorCategory,
  deleteGlossaryorType
};
