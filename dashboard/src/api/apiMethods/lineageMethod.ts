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

import { lineageApiUrl, relationsApiUrl } from "@api/apiUrlLinks/lineageApiUrl";
import { _get } from "./apiMethod";

const getLineageData = (guid: string, params: any) => {
  const config = {
    method: "GET",
    params: params
  };
  return _get(lineageApiUrl(guid), config);
};

const addLineageData = (guid: string, data: any) => {
  const config = {
    method: "POST",
    params: {},
    data: data
  };
  return _get(lineageApiUrl(guid), config);
};

const getRelationshipData = (options: any, params: any) => {
  const config = {
    method: "GET",
    params: params
  };
  return _get(relationsApiUrl(options), config);
};

const saveRelationShip = (data: any) => {
  const config = {
    method: "PUT",
    params: {},
    data: data
  };
  return _get(relationsApiUrl({}), config);
};

export {
  getLineageData,
  getRelationshipData,
  saveRelationShip,
  addLineageData
};
