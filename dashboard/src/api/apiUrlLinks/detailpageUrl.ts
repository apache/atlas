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

import { getBaseApiUrl } from "./commonApiUrl";
import { entitiesApiUrl } from "./entitiesApiUrl";

const detailpageApiUrl = (guid: string, header?: string) => {
  return header != undefined
    ? `${entitiesApiUrl()}/guid/${guid}/header`
    : `${entitiesApiUrl()}/guid/${guid}`;
};

const detailPageAuditApiUrl = (guid: string) => {
  return `${entitiesApiUrl()}/${guid}/audit`;
};

const detailPageRauditApiUrl = () => {
  return `${getBaseApiUrl("url")}/admin/expimp/audit`;
};

const auditApiurl = () => {
  return `${getBaseApiUrl("url")}/admin/audits`;
};

const detailPageLabelApiUrl = (guid: string) => {
  return `${entitiesApiUrl()}/guid/${guid}/labels`;
};

const detailPageBusinessMetadataApiUrl = (guid: string) => {
  return `${entitiesApiUrl()}/guid/${guid}/businessmetadata`;
};

const detailPageRelationshipApiUrl = (guid: string) => {
  return `${getBaseApiUrl("urlV2")}/relationship/guid/${guid}`;
};

export {
  detailpageApiUrl,
  detailPageAuditApiUrl,
  detailPageRauditApiUrl,
  auditApiurl,
  detailPageLabelApiUrl,
  detailPageBusinessMetadataApiUrl,
  detailPageRelationshipApiUrl
};
