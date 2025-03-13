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

import { fetchApi } from "./fetchApi";

interface apiObject {
  method: string;
  params: object;
  data?: any;
}

const _get = (url: string, config: apiObject) => {
  return fetchApi(url, config);
};
const _put = (url: string, config: apiObject) => {
  return fetchApi(url, config);
};

const _delete = (url: string, config: apiObject) => {
  return fetchApi(url, config);
};

const _post = (url: string, config: apiObject) => {
  return fetchApi(url, config);
};

export { _get, _delete, _put, _post };
