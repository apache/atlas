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
  detailPageRelationshipApiUrl,
  detailPageRelationshipAttributesApiUrl
} from "../apiUrlLinks/detailpageUrl";
import { _get, _post } from "./apiMethod";

const getDetailPageData = (guid: string, params: object, header?: string) => {
  const config = {
    method: "GET",
    params: { ...params, ignoreRelationships: true }
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

const getEntityHeader = (guid: string) => {
  const config = {
    method: "GET",
    params: {}
  };
  return _get(detailpageApiUrl(guid, "header"), config);
};

const getLabels = (guid: string, formData: string[]) => {
  const config = {
    method: "POST",
    params: {},
    data: formData
  };
  return _post(detailPageLabelApiUrl(guid), config);
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

// TODO: Mock data for relationship attributes (disabled now that API is used).
// const MOCK_DATA_CONFIG: Record<string, number> = {
//   tables: 500,
//   inputToProcess: 150,
//   "outputTo Process": 3,
//   ddlQueries: 230,
//   outputFromProcesses: 1,
//   model: 0
// };
//
// const getMockRelationshipAttributes = (
//   params: { limit?: number; offset?: number; attributeName?: string }
// ) => {
//   const limit = params.limit || 100;
//   const offset = params.offset || 0;
//   const attributeName = params.attributeName;
//
//   const generateMockItem = (index: number, type: string) => {
//     const cleanType = type.replace(/\s+/g, "");
//     const displayIndex = offset + index + 1;
//
//     return {
//       guid: `mock-guid-${cleanType}-${displayIndex}`,
//       typeName: "hive_table",
//       entityStatus: "ACTIVE",
//       displayText: `hivetable${displayIndex}`,
//       relationshipType: `hive_table_db`,
//       relationshipGuid: `mock-rel-guid-${cleanType}-${displayIndex}`,
//       relationshipStatus: "ACTIVE",
//       relationshipAttributes: {
//         typeName: `hive_table_db`
//       },
//       qualifiedName: `db88232_1.hivetable${displayIndex}@cm`
//     };
//   };
//
//   if (attributeName) {
//     const totalCount = MOCK_DATA_CONFIG[attributeName] || 0;
//     const remainingCount = Math.max(0, totalCount - offset);
//     const itemCount = Math.min(limit, remainingCount);
//
//     const mockData = [];
//     for (let i = 0; i < itemCount; i++) {
//       mockData.push(generateMockItem(i, attributeName));
//     }
//
//     return {
//       data: {
//         relationshipAttributes: {
//           [attributeName]: mockData
//         },
//         totalCounts: {
//           [attributeName]: totalCount
//         }
//       }
//     };
//   }
//
//   const mockResponse: any = {
//     data: {
//       relationshipAttributes: {},
//       totalCounts: {}
//     }
//   };
//
//   const relationshipTypes = [
//     "tables",
//     "inputToProcess",
//     "outputTo Process",
//     "ddlQueries",
//     "outputFromProcesses",
//     "model"
//   ];
//
//   relationshipTypes.forEach((type) => {
//     const totalCount = MOCK_DATA_CONFIG[type] || 0;
//     const itemCount = Math.min(limit, totalCount);
//
//     const mockData = [];
//     for (let i = 0; i < itemCount; i++) {
//       mockData.push(generateMockItem(i, type));
//     }
//
//     mockResponse.data.relationshipAttributes[type] = mockData;
//     mockResponse.data.totalCounts[type] = totalCount;
//   });
//
//   return mockResponse;
// };

const getDetailPageRelationshipAttributes = async (
  guid: string,
  params: { limit?: number; offset?: number; attributeName?: string }
) => {
  const config = {
    method: "GET",
    params: params
  };
  
  try {
    return await _get(detailPageRelationshipAttributesApiUrl(guid), config);
  } catch (error: any) {
    // Mock fallback disabled now that real API is used.
    // return getMockRelationshipAttributes(params);
    throw error;
  }
};

export {
  getDetailPageData,
  getDetailPageAuditData,
  getDetailPageRauditData,
  getAuditData,
  getEntityHeader,
  getLabels,
  getEntityBusinessMetadata,
  getDetailPageRelationship,
  getDetailPageRelationshipAttributes
};
