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
  businessMetadataImportTempUrl,
  businessMetadataImportUrl,
  getEntityTypeUrl
} from "../apiUrlLinks/entitiesApiUrl";
import { _get } from "./apiMethod";

interface importTmplConfigs {
  method: string;
  params: object;
}
interface importConfigs {
  method: string;
  data: object;
  params: object;
}

const getBusinessMetadataImportTmpl = (params: object) => {
  const config: importTmplConfigs = {
    method: "GET",
    params: params
  };
  return _get(businessMetadataImportTempUrl(), config);
};

const getBusinessMetadataImport = (params: object, uploadProgress: any) => {
  const config: importConfigs = {
    method: "POST",
    params: {},
    data: params,
    ...uploadProgress
  };
  return _get(businessMetadataImportUrl(), config);
};

const getEntitiesType = (name: string, params: object) => {
  const config: importTmplConfigs = {
    method: "GET",
    params: params
  };
  return _get(getEntityTypeUrl(name), config);
};

export {
  getBusinessMetadataImportTmpl,
  getBusinessMetadataImport,
  getEntitiesType
};
