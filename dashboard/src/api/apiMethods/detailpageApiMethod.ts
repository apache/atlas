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
  auditApiurl,
  detailpageApiUrl,
  detailPageAuditApiUrl,
  detailPageBusinessMetadataApiUrl,
  detailPageLabelApiUrl,
  detailPageRauditApiUrl,
  detailPageRelationshipApiUrl
} from "../apiUrlLinks/detailpageUrl";
import { _get } from "./apiMethod";

const getDetailPageData = (guid: string, params: object, header?: string) => {
  const config = {
    method: "GET",
    params: params
  };
  return _get(detailpageApiUrl(guid, header), config);
};

const getDetailPageAuditData = (guid: string, params: object) => {
  const config = {
    method: "GET",
    params: params
  };
  return _get(detailPageAuditApiUrl(guid), config);
};

const getDetailPageRauditData = (params: object) => {
  const config = {
    method: "GET",
    params: params
  };
  return _get(detailPageRauditApiUrl(), config);
};

const getAuditData = (params: object) => {
  const config = {
    method: "POST",
    params: {},
    data: params
  };
  return _get(auditApiurl(), config);
};

const getLabels = (guid: string, formData: object) => {
  const config = {
    method: "POST",
    params: {},
    data: formData
  };
  return _get(detailPageLabelApiUrl(guid), config);
};

const getEntityBusinessMetadata = (guid: string, formData: object) => {
  const config = {
    method: "POST",
    params: { isOverwrite: true },
    data: formData
  };
  return _get(detailPageBusinessMetadataApiUrl(guid), config);
};

const getDetailPageRelationship = (guid: string) => {
  const config = {
    method: "GET",
    params: {}
  };
  return _get(detailPageRelationshipApiUrl(guid), config);
};

export {
  getDetailPageData,
  getDetailPageAuditData,
  getDetailPageRauditData,
  getAuditData,
  getLabels,
  getEntityBusinessMetadata,
  getDetailPageRelationship
};
