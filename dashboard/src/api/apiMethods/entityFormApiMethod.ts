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
  geAttributeUrl,
  getEntityUrl,
  getTypedefUrl,
} from "@api/apiUrlLinks/entityFormApiUrl";
import { _get } from "./apiMethod";
import { entitiesApiUrl } from "@api/apiUrlLinks/entitiesApiUrl";

interface importTmplConfigs {
  method: string;
  params: object;
}

const getAttributes = (params: any) => {
  const config: importTmplConfigs = {
    method: "GET",
    params: params,
  };
  return _get(geAttributeUrl(), config);
};

const createEntity = (params: object) => {
  const config = {
    method: "POST",
    params: {},
    data: params,
  };
  return _get(entitiesApiUrl(), config);
};

const getEntity = (guid: string, method: string, params: any) => {
  const config: importTmplConfigs = {
    method: method,
    params: params,
  };
  return _get(getEntityUrl(guid), config);
};

const getTypedef = (typedef: string, params: any) => {
  const config: importTmplConfigs = {
    method: "GET",
    params: params,
  };
  return _get(getTypedefUrl(typedef), config);
};

export { getAttributes, createEntity, getEntity, getTypedef };
