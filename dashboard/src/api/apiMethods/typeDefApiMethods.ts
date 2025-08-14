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

import { addOnEntities } from "../../utils/Enum";
import {
  rootEntityDefUrl,
  typeDefApiUrl,
  typeDefHeaderApiUrl
} from "../apiUrlLinks/typeDefApiUrl";
import { _get, _put } from "./apiMethod";

interface configs {
  method: string;
  params: object;
}

const getTypeDefApiResp = (params: object) => {
  const config: configs = {
    method: "GET",
    params: params
  };
  return _get(typeDefApiUrl(""), config);
};

const getTypeDef = (type: string) => {
  const params = { type: type };
  return getTypeDefApiResp(params);
};

const getRootEntityDef = () => {
  const config: configs = {
    method: "GET",
    params: {}
  };
  return _get(rootEntityDefUrl(addOnEntities[0]), config);
};
const getTypeDefHeaders = () => {
  const config: configs = {
    method: "GET",
    params: { excludeInternalTypesAndReferences: true }
  };
  return _get(typeDefHeaderApiUrl(), config);
};

const createOrUpdateTag = (type: string, isAdd: boolean, params: object) => {
  const config = {
    method: isAdd ? "POST" : "PUT",
    params: { type: type },
    data: params
  };
  return _get(typeDefApiUrl(""), config);
};

const createEditBusinessMetadata = (
  type: string,
  method: string,
  params: object
) => {
  const config = {
    method: method,
    params: { type: type },
    data: params
  };
  return _get(typeDefApiUrl(""), config);
};

const updateEnum = (params: object) => {
  const config = {
    method: "PUT",
    params: {},
    data: params
  };
  return _put(typeDefApiUrl(""), config);
};

const createEnum = (params: object) => {
  const config = {
    method: "POST",
    params: {},
    data: params
  };
  return _put(typeDefApiUrl(""), config);
};
export {
  getTypeDef,
  getRootEntityDef,
  getTypeDefHeaders,
  createOrUpdateTag,
  createEditBusinessMetadata,
  updateEnum,
  createEnum
};
