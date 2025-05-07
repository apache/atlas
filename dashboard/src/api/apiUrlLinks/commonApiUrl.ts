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

export const apiBaseurl = window.location.origin;
const baseUrl = apiBaseurl + "/api/atlas";
const baseUrlV2 = apiBaseurl + "/api/atlas/v2";

const getBaseApiUrl = (url: string) => {
  if (url == "url") {
    return baseUrl;
  } else if (url == "urlV2") {
    return baseUrlV2;
  }
};

const typedefsUrl = () => {
  return {
    defs: baseUrlV2 + "/types/typedefs",
    def: baseUrlV2 + "/types/typedef"
  };
};

const getDefApiUrl = (name: string) => {
  const defApiUrl = typedefsUrl();
  let defUrl = "";
  if (name) {
    defUrl = `${defApiUrl.def}/name/${name}`;
  } else {
    defUrl = defApiUrl.defs;
  }
  return defUrl;
};
export { getBaseApiUrl, getDefApiUrl, typedefsUrl };
