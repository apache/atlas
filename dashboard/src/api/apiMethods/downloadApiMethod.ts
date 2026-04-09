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
  downloadSearchResultsCSVUrl,
  downloadSearchResultsFileUrl,
  getDownloadsListUrl
} from "@api/apiUrlLinks/downloadApiUrl";
import { _get, _post } from "./apiMethod";

const downloadSearchResultsCSV = (
  searchType: string | null,
  params: object
) => {
  const config = {
    method: "POST",
    params: {},
    data: params
  };
  return _post(downloadSearchResultsCSVUrl(searchType), config);
};

const getDownloadStatus = (params: object) => {
  const config = {
    method: "GET",
    params: params
  };
  return _get(getDownloadsListUrl(), config);
};

const downloadCVSFile = (fileName: string) => {
  const config = {
    method: "GET",
    params: {}
  };
  return _get(downloadSearchResultsFileUrl(fileName), config);
};

export { downloadSearchResultsCSV, getDownloadStatus, downloadCVSFile };
