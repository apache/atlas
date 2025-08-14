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

import moment from "moment";
import { invert } from "./Helper";

export const addOnEntities = ["_ALL_ENTITY_TYPES"];

export const addOnClassification = [
  "_ALL_CLASSIFICATION_TYPES",
  "_CLASSIFIED",
  "_NOT_CLASSIFIED"
];

export const customFilters = [
  "Advanced Search",
  "Basic Search",
  "Relationship Search"
];

export const globalSessionData: any = {};

export const entityStateReadOnly: any = {
  ACTIVE: false,
  DELETED: true,
  STATUS_ACTIVE: false,
  STATUS_DELETED: true
};

export const AdvanceSearchQueries = [
  {
    type: "Single Query",
    queries: `DB where name="Reporting" select name, owner`
  },
  {
    type: "GROUPBY",
    queries: `select count(CustomerID), Country from Customers group by Country`
  },
  {
    type: "ORDERBY",
    queries: `DB where name="Reporting" select name, owner orderby name limit 10 offset 5`
  },
  {
    type: "LIMIT",
    queries: `DB where name="Reporting" select name, owner limit 10 offset 0`
  }
];

export const isEntityPurged: { [key: string]: boolean } = {
  PURGED: true
};

export const category: { [key: string]: string } = {
  PRIMITIVE: "Primitive",
  OBJECT_ID_TYPE: "Object Id type",
  ENUM: "Enum",
  STRUCT: "Struct",
  CLASSIFICATION: "Classification",
  ENTITY: "Entity",
  ARRAY: "Array",
  MAP: "Map",
  RELATIONSHIP: "Relationship",
  BUSINESS_METADATA: "Business Metadata",
  PURGE: "Purge Entities",
  IMPORT: "Import Entities",
  EXPORT: "Export Entities"
};

export const auditAction: { [key: string]: string } = {
  ENTITY_CREATE: "Entity Created",
  ENTITY_UPDATE: "Entity Updated",
  ENTITY_DELETE: "Entity Deleted",
  CLASSIFICATION_ADD: "Classification Added",
  CLASSIFICATION_DELETE: "Classification Deleted",
  CLASSIFICATION_UPDATE: "Classification Updated",
  PROPAGATED_CLASSIFICATION_ADD: "Propagated Classification Added",
  PROPAGATED_CLASSIFICATION_DELETE: "Propagated Classification Deleted",
  PROPAGATED_CLASSIFICATION_UPDATE: "Propagated Classification Updated",
  ENTITY_IMPORT_CREATE: "Entity Created by import",
  ENTITY_IMPORT_UPDATE: "Entity Updated by import",
  ENTITY_IMPORT_DELETE: "Entity Deleted by import",
  TERM_ADD: "Term Added",
  TERM_DELETE: "Term Deleted",
  LABEL_ADD: "Label(s) Added",
  LABEL_DELETE: "Label(s) Deleted",
  ENTITY_PURGE: "Entity Purged",
  BUSINESS_ATTRIBUTE_ADD: "Business Attribute(s) Added",
  BUSINESS_ATTRIBUTE_UPDATE: "Business Attribute(s) Updated",
  BUSINESS_ATTRIBUTE_DELETE: "Business Attribute(s) Deleted",
  CUSTOM_ATTRIBUTE_UPDATE: "User-defined Attribute(s) Updated",
  TYPE_DEF_UPDATE: "Type Updated",
  TYPE_DEF_CREATE: "Type Created",
  TYPE_DEF_DELETE: "Type Deleted",
  IMPORT: "Import",
  EXPORT: "Export"
};

export const stats: any = {
  generalData: {
    collectionTime: "day"
  },
  Server: {
    startTimeStamp: "day",
    activeTimeStamp: "day",
    upTime: "none"
  },
  ConnectionStatus: {
    statusBackendStore: "status-html",
    statusIndexStore: "status-html"
  },
  Notification: {
    currentDay: "number",
    currentDayAvgTime: "number",
    currentDayEntityCreates: "number",
    currentDayEntityDeletes: "number",
    currentDayEntityUpdates: "number",
    currentDayFailed: "number",
    currentDayStartTime: "day",
    currentHour: "number",
    currentHourAvgTime: "millisecond",
    currentHourEntityCreates: "number",
    currentHourEntityDeletes: "number",
    currentHourEntityUpdates: "number",
    currentHourFailed: "number",
    currentHourStartTime: "day",
    lastMessageProcessedTime: "day",
    offsetCurrent: "number",
    offsetStart: "number",
    previousDay: "number",
    previousDayAvgTime: "millisecond",
    previousDayEntityCreates: "number",
    previousDayEntityDeletes: "number",
    previousDayEntityUpdates: "number",
    previousDayFailed: "number",
    previousHour: "number",
    previousHourAvgTime: "millisecond",
    previousHourEntityCreates: "number",
    previousHourEntityDeletes: "number",
    previousHourEntityUpdates: "number",
    previousHourFailed: "number",
    total: "number",
    totalAvgTime: "millisecond",
    totalCreates: "number",
    totalDeletes: "number",
    totalFailed: "number",
    totalUpdates: "number",
    processedMessageCount: "number",
    failedMessageCount: "number"
  }
};

const getTermRelationAttributes = () => {
  return {
    description: null,
    expression: null,
    steward: null,
    source: null
  };
};

export const termRelationAttributeList = {
  seeAlso: getTermRelationAttributes(),
  synonyms: getTermRelationAttributes(),
  antonyms: getTermRelationAttributes(),
  preferredTerms: getTermRelationAttributes(),
  preferredToTerms: getTermRelationAttributes(),
  replacementTerms: getTermRelationAttributes(),
  replacedBy: getTermRelationAttributes(),
  translationTerms: getTermRelationAttributes(),
  translatedTerms: getTermRelationAttributes(),
  isA: getTermRelationAttributes(),
  classifies: getTermRelationAttributes(),
  validValues: getTermRelationAttributes(),
  validValuesFor: getTermRelationAttributes()
};

export const defaultDataType = [
  "string",
  "boolean",
  "byte",
  "short",
  "int",
  "float",
  "double",
  "array<string>"
];

export const dataTypes = [
  "string",
  "boolean",
  "byte",
  "short",
  "int",
  "float",
  "double",
  "long",
  "date",
  "enumeration"
];

export const searchWeight = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

export const attributeObj = {
  description: "",
  expression: "",
  steward: "",
  source: ""
};

export const systemAttributes: Record<string, string> = {
  __classificationNames: "Classification(s)",
  __createdBy: "Created By User",
  __customAttributes: "User-defined Properties",
  __guid: "Guid",
  __isIncomplete: "IsIncomplete",
  __labels: "Label(s)",
  __modificationTimestamp: "Last Modified Timestamp",
  __modifiedBy: "Last Modified User",
  __propagatedClassificationNames: "Propagated Classification(s)",
  __state: "Status",
  __entityStatus: "Entity Status",
  __timestamp: "Created Timestamp",
  __typeName: "Type Name"
};

export const regex = {
  RANGE_CHECK: {
    byte: {
      min: -128,
      max: 127
    },
    short: {
      min: -32768,
      max: 32767
    },
    int: {
      min: -2147483648,
      max: 2147483647
    },
    long: {
      min: -9223372036854775808,
      max: 9223372036854775807
    },
    float: {
      min: -3.4028235e38,
      max: 3.4028235e38
    },
    double: {
      min: -1.7976931348623157e308,
      max: 1.7976931348623157e308
    }
  }
};

export const dateRangesMap = {
  Today: [moment(), moment()],
  Yesterday: [moment().subtract(1, "days"), moment().subtract(1, "days")],
  "Last 7 Days": [moment().subtract(6, "days"), moment()],
  "Last 30 Days": [moment().subtract(29, "days"), moment()],
  "This Month": [moment().startOf("month"), moment().endOf("month")],
  "Last Month": [
    moment().subtract(1, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 3 Months": [
    moment().subtract(3, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 6 Months": [
    moment().subtract(6, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 12 Months": [
    moment().subtract(12, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "This Quarter": [moment().startOf("quarter"), moment().endOf("quarter")],
  "Last Quarter": [
    moment().subtract(1, "quarter").startOf("quarter"),
    moment().subtract(1, "quarter").endOf("quarter")
  ],
  "This Year": [moment().startOf("year"), moment().endOf("year")],
  "Last Year": [
    moment().subtract(1, "year").startOf("year"),
    moment().subtract(1, "year").endOf("year")
  ]
};

export const queryBuilderUIOperatorToAPI = {
  "=": "eq",
  "!=": "neq",
  "<": "lt",
  "<=": "lte",
  ">": "gt",
  ">=": "gte",
  begins_with: "startsWith",
  ends_with: "endsWith",
  not_null: "notNull",
  is_null: "isNull",
  TIME_RANGE: "timerange"
};

export const queryBuilderApiOperatorToUI = invert(queryBuilderUIOperatorToAPI);

export const queryBuilderDateRangeUIValueToAPI: Record<string, string> = {
  Today: "TODAY",
  Yesterday: "YESTERDAY",
  "Last 7 Days": "LAST_7_DAYS",
  "Last 30 Days": "LAST_30_DAYS",
  "This Month": "THIS_MONTH",
  "Last Month": "LAST_MONTH",
  "This Quarter": "THIS_QUARTER",
  "Last Quarter": "LAST_QUARTER",
  "This Year": "THIS_YEAR",
  "Last Year": "LAST_YEAR",
  "Last 3 Months": "LAST_3_MONTHS",
  "Last 6 Months": "LAST_6_MONTHS",
  "Last 12 Months": "LAST_12_MONTHS"
};

export const queryBuilderDateRangeAPIValueToUI = invert(
  queryBuilderDateRangeUIValueToAPI
);

export const graphIcon = {
  // hive_db: { icon: "fa-database", textContent: "\uf1c0" },
  // hive_column: { icon: "fa-columns", textContent: "\uf0db" },
  // hive_table: { icon: "fa-table", textContent: "\uf0ce" },
};

export const defaultAttrObj = {
  name: "",
  typeName: "string",
  searchWeight: 5,
  multiValueSelect: false,
  options: {
    maxStrLength: 50,
    applicableEntityTypes: null
  },
  isOptional: true,
  cardinality: "SINGLE",
  valuesMinCount: 0,
  valuesMaxCount: 1,
  isUnique: false,
  isIndexable: true
};

export const filterQueryValue: any = {};

export const extractFromUrlForSearch = {
  searchParameters: {
    pageLimit: "limit",
    type: "typeName",
    relationshipName: "relationshipName",
    tag: "classification",
    query: "query",
    pageOffset: "offset",
    includeDE: "excludeDeletedEntities",
    excludeST: "includeSubTypes",
    excludeSC: "includeSubClassifications",
    tagFilters: "tagFilters",
    entityFilters: "entityFilters",
    relationshipFilters: "relationshipFilters",
    attributes: "attributes",
    term: "termName"
  },
  uiParameters: "uiParameters"
};

export const timeRangeOptions = [
  { label: "Today", value: "Today" },
  { label: "Yesterday", value: "Yesterday" },
  { label: "Last 7 days", value: "LAST_7_DAYS" },
  { label: "Last 30 days", value: "LAST_30_DAYS" },
  { label: "This Month", value: "THIS_MONTH" },
  { label: "Last 3 Months", value: "LAST_3_MONTHS" },
  { label: "Last 6 Months", value: "LAST_6_MONTHS" },
  { label: "Last 12 Months", value: "LAST_12_MONTHS" },
  { label: "This Quarter", value: "THIS_QUARTER" },
  { label: "Last Quarter", value: "LAST_QUARTER" },
  { label: "This Year", value: "THIS_YEAR" },
  { label: "Last Year", value: "LAST_YEAR" },
  { label: "Custom Range", value: "CUSTOM_RANGE" }
];

export const statsDateRangesMap: Record<string, any> = {
  Today: moment(),
  "1d": [moment().subtract(1, "days"), moment()],
  "7d": [moment().subtract(6, "days"), moment()],
  "14d": [moment().subtract(13, "days"), moment()],
  "30d": [moment().subtract(29, "days"), moment()],
  "This Month": [moment().startOf("month"), moment().endOf("month")],
  "Last Month": [
    moment().subtract(1, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 3 Months": [
    moment().subtract(3, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 6 Months": [
    moment().subtract(6, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 12 Months": [
    moment().subtract(12, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "This Quarter": [moment().startOf("quarter"), moment().endOf("quarter")],
  "Last Quarter": [
    moment().subtract(1, "quarter").startOf("quarter"),
    moment().subtract(1, "quarter").endOf("quarter")
  ],
  "This Year": [moment().startOf("year"), moment().endOf("year")],
  "Last Year": [
    moment().subtract(1, "year").startOf("year"),
    moment().subtract(1, "year").endOf("year")
  ]
};

export const serviceTypeMap: any = {};

export const lineageDepth = 3;

export const PathAssociateWithModule = {
  EntityDetails: ["/detailPage/:guid"],
  ClassificationDetails: ["/tag/tagAttribute/:tagName"],
  GLossaryDetails: ["/glossary/:guid"],
  Administratior: ["/administrator"],
  BusinessMetadataDetails: ["/administrator/businessMetadata/:bmguid"],
  RelationshipDetails: ["/relationshipDetailPage/:guid"],
  DebugMetrics: ["/debugMetrics"],
  Dashboard: ["/search"],
  LandingPage: ["/"],
  SearchResult: ["/search/searchResult"],
  RelationShipSearch: ["/relationship/relationshipSearchResult"],
  DataNotFound: ["/dataNotFound"],
  PageNotFound: ["/pageNotFound"],
  Forbidden: ["/forbidden"]
};

export const defaultType = [
  "string",
  "boolean",
  "byte",
  "short",
  "int",
  "float",
  "double",
  "long",
  "date",
  "array<string>",
  "array<boolean>",
  "array<byte>",
  "array<short>",
  "array<int>",
  "array<float>",
  "array<double>",
  "array<long>",
  "array<date>"
];
