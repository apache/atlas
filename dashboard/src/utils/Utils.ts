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

import { toast } from "react-toastify";
import { entityStateReadOnly, globalSessionData } from "./Enum";
import { dateTimeFormat, entityImgPath } from "./Global";
import moment from "moment-timezone";
import sanitizeHtml from "sanitize-html";
import { cloneDeep, toArrayifObject, uniq } from "./Helper";
import { attributeFilter } from "./CommonViewFunction";
import { Messages } from "./Messages";

interface childrenInterface {
  gType: string;
  guid: string;
  id: string;
  name: string;
  text: string;
  type: string;
}
interface serviceTypeInterface {
  [key: string]: {
    children: childrenInterface[];
    name: string;
    totalCount: number;
  };
}
const customSortBy = (
  array: { id: string; label: string }[] | any,
  keys: string[]
) =>
  [...array]?.sort(
    (a: { id: string; label: string }, b: { id: string; label: string }) =>
      keys.reduce((result, key) => {
        if (result !== 0) return result;
        return a[key as keyof { id: string; label: string }] <
          b[key as keyof { id: string; label: string }]
          ? -1
          : a[key as keyof { id: string; label: string }] >
            b[key as keyof { id: string; label: string }]
          ? 1
          : 0;
      }, 0)
  );

const customSortByObjectKeys = (
  array: serviceTypeInterface[] | any
): serviceTypeInterface[] | any => {
  return [...array]?.sort((a: any, b: any) => {
    const keysA = Object.keys(a);
    const keysB = Object.keys(b);
    keysA.sort();
    keysB.sort();
    const keyA = keysA[0];
    const keyB = keysB[0];

    return keyA?.localeCompare(keyB);
  });
};

const removeDuplicateObjects = (AllColumns: any[]) => {
  return AllColumns?.filter(Boolean).filter(
    (obj: any, index: any, arr: any[]) => {
      return (
        arr.findIndex((o) => {
          return (
            JSON.stringify(o.accessorKey) === JSON.stringify(obj.accessorKey)
          );
        }) === index
      );
    }
  );
};

const getBoolean = (value: string) => {
  return value == "false" ? false : true;
};

const isNull = (data: any) => {
  if (data === null) {
    return true;
  } else {
    return false;
  }
};

const isArray = (data: any) => {
  if (Array.isArray(data)) {
    return true;
  } else {
    return false;
  }
};
const isEmpty = (value: any) => {
  return (
    value === undefined ||
    value === null ||
    (typeof value === "object" && Object.keys(value).length === 0) ||
    (typeof value === "string" && value.trim().length === 0)
  );
};

const isObject = (obj: any) => {
  return obj !== null && typeof obj === "object";
};

const isString = (obj: any) => {
  return obj !== null && typeof obj === "string";
};

const isNumber = (obj: any) => {
  return obj !== null && typeof obj === "number";
};

const isBoolean = (obj: any) => {
  return obj !== null && typeof obj === "boolean";
};

const isFunction = (obj: any) => {
  return obj !== null && typeof obj === "function";
};

const pick = (obj: { [x: string]: any }, keys: any[]) =>
  keys.reduce(
    (acc, key) => (obj[key] != undefined && (acc[key] = obj[key]), acc),
    {}
  );
const findWhere = (array: any[], criteria: { [x: string]: any }) => {
  return !isEmpty(array)
    ? array.find((item) =>
        Object.keys(criteria).every((key) => item[key] === criteria[key])
      )
    : {};
};
const findUniqueValues = (array1: any, array2: any) => {
  const uniqueValues: any = [];
  array1.forEach((value: any) => {
    if (!array2.includes(value)) {
      uniqueValues.push(value);
    }
  });

  return uniqueValues;
};

const getBaseUrl = (url: string, noPop?: boolean | undefined) => {
  var path = url.replace(/\/[\w-]+.(jsp|html)|\/+$/gi, ""),
    splitPath = path.split("/");
  if (
    noPop !== true &&
    splitPath &&
    (splitPath[splitPath.length - 1] === "n" ||
      splitPath[splitPath.length - 1] === "n3")
  ) {
    splitPath.pop();
    return splitPath.join("/");
  }
  return path;
};

const extractKeyValueFromEntity = (
  data: any,
  priorityAttribute?: any,
  skipAttribute?: any,
  getGuid?: any,
  headerData?: any
) => {
  let collectionJSON = cloneDeep(data);
  let returnObj: { name: string; found: boolean; key: null | string } = {
    name: "-",
    found: true,
    key: null
  };
  const attributeOrder = [
    "name",
    "displayName",
    "qualifiedName",
    "displayText",
    "guid",
    "id"
  ];
  if (collectionJSON?.attributes?.[priorityAttribute]) {
    returnObj.name = collectionJSON.attributes[priorityAttribute];
    returnObj.key = priorityAttribute;
    return returnObj;
  }
  if (collectionJSON?.[priorityAttribute]) {
    returnObj.name = collectionJSON[priorityAttribute];
    returnObj.key = priorityAttribute;
    return returnObj;
  }
  for (const attribute of attributeOrder) {
    if (collectionJSON?.attributes?.[attribute]) {
      if (
        attribute === "id" &&
        typeof collectionJSON.attributes.id === "object"
      ) {
        returnObj.name = collectionJSON.attributes.id.id;
      } else {
        returnObj.name = collectionJSON.attributes[attribute];
      }
      returnObj.key = attribute;
      return returnObj;
    }
  }

  const propertyOrder = [
    "name",
    "displayName",
    "qualifiedName",
    "displayText",
    "guid",
    "id"
  ];

  for (const property of propertyOrder) {
    if (collectionJSON?.[property]) {
      if (property === "id" && typeof collectionJSON.id === "object") {
        returnObj.name = collectionJSON.id.id;
      } else if (property == "guid" && !isEmpty(getGuid)) {
        getGuid(collectionJSON[property]);
        if (headerData != undefined) {
          returnObj.name = headerData;
        }
      } else {
        returnObj.name = collectionJSON[property];
      }
      returnObj.key = property;
      return returnObj;
    }
  }

  returnObj.found = false;
  if (skipAttribute && returnObj.key == skipAttribute) {
    return {
      name: "-",
      found: true,
      key: null
    };
  } else {
    return returnObj;
  }
};

const getNestedSuperTypes = (options: { data: any; collection: any }) => {
  let data = options.data,
    collection = options.collection,
    superTypes: any[] = [];

  const getTypeData = function (data: any, collection: any) {
    if (data) {
      superTypes = superTypes.concat(data.superTypes);
      if (data.superTypes && data.superTypes.length) {
        let collectionData;
        data.superTypes.forEach((superTypeName: string) => {
          if (collection) {
            collectionData = findWhere(collection, { name: superTypeName });
          } else {
            collectionData = findWhere(collection, { name: superTypeName });
          }
          getTypeData(collectionData, collection);
        });
      }
    }
  };
  getTypeData(data, collection);
  return superTypes;
};

const getEntityIconPath = (options: any) => {
  let entityData = options && options.entityData,
    serviceType: string,
    status: any,
    typeName: string,
    iconBasePath: string = getBaseUrl(window.location.pathname) + entityImgPath;

  typeName = entityData.typeName;
  serviceType = entityData && entityData.serviceType;
  status = entityData && entityData.status;

  const getImgPath = (imageName: string) => {
    return (
      iconBasePath +
      (entityStateReadOnly[status] ? "disabled/" + imageName : imageName)
    );
  };

  function getDefaultImgPath() {
    if (entityData.isProcess) {
      if (entityStateReadOnly[status]) {
        return iconBasePath + "disabled/process.png";
      } else {
        return iconBasePath + "process.png";
      }
    } else {
      if (entityStateReadOnly[status]) {
        return iconBasePath + "disabled/table.png";
      } else {
        return iconBasePath + "table.png";
      }
    }
  }

  if (entityData) {
    if (options.errorUrl) {
      var isErrorInTypeName =
        options.errorUrl &&
        options.errorUrl.match(
          "entity-icon/" + typeName + ".png|disabled/" + typeName + ".png"
        )
          ? true
          : false;
      if (serviceType && isErrorInTypeName) {
        var imageName = serviceType + ".png";
        return getImgPath(imageName);
      } else {
        return getDefaultImgPath();
      }
    } else if (entityData.typeName) {
      var imageName = entityData.typeName + ".png";
      return getImgPath(imageName);
    } else {
      return getDefaultImgPath();
    }
  }
};

const serverError = (error: any, toastId: any) => {
  if (
    error.response !== undefined &&
    error.response.data.errorMessage !== undefined
  ) {
    toast.dismiss(toastId.current);
    toastId.current = toast.error(error.response.data.errorMessage);
  } else if (
    error.response !== undefined &&
    error.response.data !== undefined
  ) {
    toast.dismiss(toastId.current);
    toastId.current = toast.error(error.response.data);
  } else if (
    error.response !== undefined &&
    error.response.data.msgDesc !== undefined
  ) {
    toast.dismiss(toastId.current);
    toastId.current = toast.error(error.response.data.msgDesc);
  }
};

const dateFormat = (date: number) => {
  const { isTimezoneFormatEnabled = "" } = globalSessionData || {};

  if (isTimezoneFormatEnabled) {
    return date != 0
      ? `${moment(date).format(dateTimeFormat)} (${moment
          .tz(moment.tz.guess())
          .zoneAbbr()})`
      : `${moment().format(dateTimeFormat)} (${moment
          .tz(moment.tz.guess())
          .zoneAbbr()})`;
  }

  return date != 0
    ? `${moment(date).format(dateTimeFormat)}`
    : `${moment().format(dateTimeFormat)}`;
};

const formatedDate = function (options: any) {
  let dateValue: any = null,
    dateFormat = dateTimeFormat,
    isValidDate = false;
  const { isTimezoneFormatEnabled = "" } = globalSessionData || {};

  if (options && !options.date) {
    return "N/A";
  } else {
    dateValue = options.date;
    if (dateValue !== "-") {
      dateValue = parseInt(dateValue);
      if (isNaN(dateValue)) {
        dateValue = options.date;
      }
      dateValue = moment(dateValue);
      if (dateValue._isValid) {
        isValidDate = true;
        dateValue = dateValue.format(dateFormat);
      }
    }
  }
  if (dateValue !== "-") {
    if (isValidDate === false && options && options.defaultDate !== false) {
      dateValue = moment().format(dateFormat);
    }
    if (isTimezoneFormatEnabled) {
      if (!options || (options && options.zone !== false)) {
        dateValue += " (" + moment.tz(moment.tz.guess()).zoneAbbr() + ")";
      }
    }
  }
  return dateValue;
};

function flattenArray(arr: any) {
  let flattened: any = [];

  arr?.forEach((item: any) => {
    if (Array.isArray(item)) {
      flattened.push(...flattenArray(item));
    } else {
      flattened.push(item);
    }
  });

  return flattened;
}

const Capitalize = (stringData: string) => {
  return stringData[0].toUpperCase() + stringData.slice(1);
};

let groupBy = function (xs: any[], key: string) {
  return xs.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
};

const noTreeData = () => {
  return [{ id: "No Records Found", label: "No Records Found" }];
};

const sanitizeHtmlContent = (htmlContent: HTMLElement | string | any) => {
  let cleanContent = sanitizeHtml(htmlContent, {
    allowedTags: [
      "b",
      "em",
      "strong",
      "u",
      "a",
      "ul",
      "ol",
      "li",
      "p",
      "strike",
      "h1",
      "h2",
      "h3",
      "h4"
    ],
    allowedAttributes: {
      a: ["href"]
    },
    allowedSchemesByTag: ["script", "img", "iframe", "svg", "title"] as any
  });
  return cleanContent;
};

const getTagObj = (entity: any, classifications: any) => {
  let tags: any = {
    self: [],
    propagated: [],
    propagatedMap: {},
    combineMap: {},
    entity: entity
  };

  if (!isEmpty(classifications)) {
    let newClassifications: any[] = structuredClone(classifications);
    for (let val in newClassifications) {
      let typeName: string = newClassifications[val].typeName;
      if (newClassifications[val].entityGuid === entity.guid) {
        tags.self.push(newClassifications[val]);
      } else {
        tags.propagated.push(newClassifications[val]);
        if (tags.propagatedMap[typeName]) {
          tags.propagatedMap[typeName].count++;
        } else {
          tags.propagatedMap[typeName] = newClassifications[val];
          tags.propagatedMap[typeName].count = 1;
        }
      }
      if (!isEmpty(tags.combineMap[typeName])) {
        tags.combineMap[typeName] = newClassifications[val];
      }
    }
  }
  return tags;
};

const millisecondsToTime = (duration: number | moment.Duration) => {
  let milliseconds = parseInt((((duration as number) % 1000) / 100) as any),
    seconds: number | string = parseInt(
      (((duration as number) / 1000) % 60) as any
    ),
    minutes: number | string = parseInt(
      (((duration as number) / (1000 * 60)) % 60) as any
    ),
    hours: number | string = parseInt(
      (((duration as number) / (1000 * 60 * 60)) % 24) as any
    );

  hours = hours < 10 ? "0" + hours : hours;
  minutes = minutes < 10 ? "0" + minutes : minutes;
  seconds = seconds < 10 ? "0" + seconds : seconds;

  return hours + ":" + minutes + ":" + seconds + "." + milliseconds;
};

export const jsonParse = (jsonObj: any) => {
  return !isEmpty(jsonObj)
    ? JSON.parse(jsonObj, (_key, value) => {
        try {
          return JSON.parse(value);
        } catch (e) {
          return value;
        }
      })
    : [];
};

export const getNestedSuperTypeObj = (options: any) => {
  let mainData = options.data,
    collection = options.collection,
    attrMerge = options.attrMerge,
    seperateRelatioshipAttr = options.seperateRelatioshipAttr || false,
    mergeRelationAttributes =
      options.mergeRelationAttributes ||
      (seperateRelatioshipAttr ? false : true);

  if (mergeRelationAttributes && seperateRelatioshipAttr) {
    throw "Both mergeRelationAttributes & seperateRelatioshipAttr cannot be true!";
  }
  let attributeDefs: any = {};
  if (attrMerge && !seperateRelatioshipAttr) {
    attributeDefs = [];
  } else if (options.attrMerge && seperateRelatioshipAttr) {
    attributeDefs = {
      attributeDefs: [],
      relationshipAttributeDefs: []
    };
  }
  const getRelationshipAttributeDef = (data: {
    relationshipAttributeDefs: any;
  }) => {
    return !isEmpty(data.relationshipAttributeDefs)
      ? data.relationshipAttributeDefs.filter((obj: any) => {
          return obj;
        })
      : [];
  };

  const getData = (data: any, collection: any) => {
    if (options.attrMerge) {
      if (seperateRelatioshipAttr) {
        attributeDefs.attributeDefs = attributeDefs.attributeDefs.concat(
          data.attributeDefs
        );
        attributeDefs.relationshipAttributeDefs =
          attributeDefs.relationshipAttributeDefs.concat(
            getRelationshipAttributeDef(data)
          );
      } else {
        attributeDefs = attributeDefs.concat(data.attributeDefs);
        if (mergeRelationAttributes) {
          attributeDefs = attributeDefs.concat(
            getRelationshipAttributeDef(data)
          );
        }
      }
    } else {
      if (attributeDefs[data.name]) {
        attributeDefs[data.name] = toArrayifObject(
          attributeDefs[data.name]
        ).concat(data.attributeDefs);
      } else {
        if (seperateRelatioshipAttr) {
          attributeDefs[data.name] = {
            attributeDefs: data.attributeDefs,
            relationshipAttributeDefs: data.relationshipAttributeDefs
          };
        } else {
          attributeDefs[data.name] = data.attributeDefs;
          if (mergeRelationAttributes) {
            attributeDefs[data.name] = toArrayifObject(
              attributeDefs[data.name]
            ).concat(getRelationshipAttributeDef(data));
          }
        }
      }
    }
    if (data.superTypes && data.superTypes.length) {
      return data.superTypes.forEach((superTypeName: string) => {
        if (collection) {
          var collectionData = findWhere(collection, {
            name: superTypeName
          });
        } else {
          var collectionData = findWhere(collection, {
            name: superTypeName
          });
        }
        collectionData =
          collectionData && collectionData.toJSON
            ? collectionData.toJSON()
            : collectionData;
        if (collectionData) {
          return getData(collectionData, collection);
        } else {
          return;
        }
      });
    }
  };
  getData(mainData, collection);

  if (attrMerge) {
    if (seperateRelatioshipAttr) {
      attributeDefs = {
        attributeDefs: uniq(
          customSortBy(attributeDefs.attributeDefs, ["name"]),
          true,
          function (obj: { name: string }) {
            return obj.name;
          }
        ),
        relationshipAttributeDefs: uniq(
          customSortBy(attributeDefs.relationshipAttributeDefs, ["name"]),
          true,
          function (obj: { name: string; relationshipTypeName: string }) {
            return obj.name + obj.relationshipTypeName;
          }
        )
      };
    } else {
      attributeDefs = uniq(
        customSortBy(attributeDefs, ["name"]),
        true,
        function (obj: { name: string; relationshipTypeName: string }) {
          if (obj?.relationshipTypeName) {
            return obj.name + obj.relationshipTypeName;
          } else {
            return obj?.name;
          }
        }
      );
    }
  }

  return attributeDefs;
};

const convertToValidDate = (dateValue: string): any => {
  let value = dateValue.split(" ");
  let dateSplit: any =
    value[0].indexOf("-") == -1 ? value[0].split("/") : value[0].split("-");
  if (value.length > 1) {
    let time: any = value[1].split(":");
    return new Date(
      dateSplit[2],
      Number(dateSplit[1]) - 1,
      dateSplit[0],
      time[0],
      time[1],
      time[2]
    );
  } else {
    return new Date(dateSplit[2], Number(dateSplit[1]) - 1, dateSplit[0]);
  }
};

const getUrlState = {
  getQueryUrl: (url: string) => {
    var hashValue = window.location.hash;
    if (url) {
      hashValue = url;
    }
    return {
      firstValue: hashValue.split("/")[1],
      hash: hashValue,
      queyParams: hashValue.split("?"),
      lastValue: hashValue.split("/")[hashValue.split("/").length - 1]
    };
  },
  checkTabUrl: (options: any) => {
    var url = options && options.url,
      matchString = options && options.matchString,
      quey = getUrlState.getQueryUrl(url);
    return (
      quey.firstValue == matchString ||
      quey.queyParams[0] == "#!/" + matchString
    );
  },
  isRelationTab: (url: string) => {
    return getUrlState.checkTabUrl({
      url: url,
      matchString: "relationship"
    });
  },
  isRelationSearch: (url?: string) => {
    return getUrlState.checkTabUrl({
      url: url,
      matchString: "relationship/relationshipSearchResult"
    });
  },
  getQueryParams: function (url: string) {
    var qs = this.getQueryUrl(url).queyParams[1];
    if (typeof qs == "string") {
      qs = qs.split("+").join(" ");
      var params: any = {},
        tokens,
        re = /[?&]?([^=]+)=([^&]*)/g;
      while ((tokens = re.exec(qs))) {
        params[decodeURIComponent(tokens[1])] = decodeURIComponent(tokens[2]);
      }
      return params;
    }
  }
};

const searchParamsAPiQuery = (rules: string | null) => {
  let ruleObj = attributeFilter.generateAPIObj(rules);
  return ruleObj;
};

const serverErrorHandler = function (
  response: { responseJSON: any },
  defaultErrorMessage: any
) {
  var responseJSON = response ? response.responseJSON : response,
    message = defaultErrorMessage
      ? defaultErrorMessage
      : Messages.defaultErrorMessage;
  if (response && responseJSON) {
    message =
      responseJSON.errorMessage ||
      responseJSON.message ||
      responseJSON.error ||
      message;
  }
  const existingError =
    document?.querySelector(".Toastify__toast--error .Toastify__toast-body")
      ?.textContent || "";
  if (existingError !== message) {
    toast.error(message);
  }
};

let backNavigate: string = "";

const GlobalQueryState = {
  query: {},
  setQuery: function (newQuery: {}) {
    this.query = newQuery;
  },
  getQuery: function () {
    return this.query;
  }
};

const setNavigate = (url: string) => {
  backNavigate = url;
};

const getNavigate = () => {
  return backNavigate;
};

const globalSearchFilterInitialQuery: any = {
  query: {},
  setQuery: (newQuery: any) => {
    globalSearchFilterInitialQuery.query = {
      ...globalSearchFilterInitialQuery.query,
      ...newQuery
    };
  },
  getQuery: () => {
    return globalSearchFilterInitialQuery.query;
  }
};

const globalSearchParams = {
  basicParams: {},
  dslParams: {}
};

export {
  customSortBy,
  customSortByObjectKeys,
  removeDuplicateObjects,
  isNull,
  isArray,
  isEmpty,
  findUniqueValues,
  extractKeyValueFromEntity,
  isObject,
  isString,
  isBoolean,
  isNumber,
  getEntityIconPath,
  serverError,
  dateFormat,
  flattenArray,
  Capitalize,
  groupBy,
  isFunction,
  getBoolean,
  noTreeData,
  pick,
  getNestedSuperTypes,
  findWhere,
  sanitizeHtmlContent,
  getTagObj,
  millisecondsToTime,
  formatedDate,
  convertToValidDate,
  getUrlState,
  searchParamsAPiQuery,
  getBaseUrl,
  serverErrorHandler,
  GlobalQueryState,
  setNavigate,
  getNavigate,
  globalSearchParams,
  globalSearchFilterInitialQuery
};
