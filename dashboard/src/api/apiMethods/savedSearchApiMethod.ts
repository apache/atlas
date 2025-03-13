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

import { CustomFiltersNodeType } from "../../models/customFiltersType";
import { getSavedSearchUrl } from "../apiUrlLinks/savedSearchApiUrl";
import { _delete, _get, _put } from "./apiMethod";

interface configs {
  method: string;
  params: object;
}

const getSavedSearch = () => {
  const config: configs = {
    method: "GET",
    params: {}
  };
  return _get(getSavedSearchUrl(), config);
};

const removeSavedSearch = (guid: string) => {
  return _delete(`${getSavedSearchUrl()}/${guid}`, {
    method: "DELETE",
    params: {}
  });
};

const editSavedSearch = (data: CustomFiltersNodeType, method: string) => {
  return _put(getSavedSearchUrl(), {
    method: method,
    params: {},
    data: data
  });
};

export { getSavedSearch, removeSavedSearch, editSavedSearch };
