// @ts-nocheck

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
  extractFromUrlForSearch,
  queryBuilderApiOperatorToUI,
  queryBuilderDateRangeAPIValueToUI,
  queryBuilderDateRangeUIValueToAPI,
  queryBuilderUIOperatorToAPI
} from "./Enum";
import {
  convertToValidDate,
  formatedDate,
  getUrlState,
  isEmpty,
  isObject,
  isString
} from "./Utils";

export const getValue = (value: unknown, _val?: string, _key?: string) => {
  if (value != undefined && value != null) {
    return value;
  } else {
    return "NA";
  }
};

export const JSONPrettyPrint = (obj: object) => {
  var replacer = function (
      _match: any,
      pIndent: any,
      pKey: any,
      pVal: any,
      pEnd: any
    ) {
      var key = `<span class=json-key>`;
      var val = `<span class=json-value>`;
      var str = `<span class=json-string>`;
      var r = pIndent || "";
      if (pKey) r = r + key + pKey.replace(/[": ]/g, "") + "</span>: ";
      if (pVal)
        r = r + (pVal[0] == '"' ? str : val) + getValue(pVal, "") + "</span>";
      return r + (pEnd || "");
    },
    jsonLine = /^( *)("[\w]+": )?("[^"]*"|[\w.+-]*)?([,[{])?$/gm;
  if (obj && isObject(obj)) {
    return JSON.stringify(obj, null, 3)
      .replace(/&/g, "&amp;")
      .replace(/\\"/g, "&quot;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(jsonLine, replacer);
  } else {
    return {};
  }
};

export const attributeFilter = {
  generateUrl: (options) => {
    let attrQuery = [];
    let attrObj = options.value;
    let formatedDateToLong = options.formatedDateToLong;
    let attributeDefs = options.attributeDefs;
    let spliter = 1;

    const mapApiOperatorToUI = (oper) => {
      return queryBuilderApiOperatorToUI[oper] || oper;
    };
    const conditionalURl = (options, splitter) => {
      if (!isEmpty(options)) {
        return (options.rules || options.criterion)
          .map((obj) => {
            if (obj.hasOwnProperty("condition")) {
              return (
                obj.condition + "(" + conditionalURl(obj, spliter + 1) + ")"
              );
            }
            if (attributeDefs) {
              var attributeDef = !isEmpty(attributeDefs)
                ? attributeDefs.find((attr) => attr.name == obj.attributeName)
                : {};
              if (attributeDef) {
                obj.attributeValue = obj.attributeValue;
                obj["attributeType"] = attributeDef.typeName;
              }
            }
            let type = obj.type || obj.attributeType;
            let value =
              (isString(obj.value) &&
                ["is_null", "not_null"].includes(obj.operator) &&
                type === "date") ||
              isObject(obj.value)
                ? ""
                : typeof (obj.value || obj.attributeValue) === "string"
                ? (obj.value || obj.attributeValue).trim()
                : obj.value || obj.attributeValue;
            let url = [
              obj.field || obj.attributeName,
              mapApiOperatorToUI(obj.operator),
              value
            ];

            if (obj.operator === "TIME_RANGE") {
              if (value.indexOf("-") > -1) {
                url[2] = value
                  .split(" - ")
                  .map(function (udKey) {
                    return Date.parse(
                      convertToValidDate(udKey?.trim())
                    ).toString();
                    // : Date.parse(udKey.trim()).toString();
                  })
                  .join(",");
              } else {
                url[2] =
                  queryBuilderDateRangeUIValueToAPI[value?.trim()] || value;
              }
            } else if (
              value &&
              value.length &&
              type === "date" &&
              formatedDateToLong
            ) {
              url[2] = Date.parse(convertToValidDate(value));
            }
            if (type) {
              if (type == "date") {
                url.push("date");
              }
              if (type) {
                url.push(type);
              }
            }
            return url.join("::");
          })
          .join("|" + spliter + "|");
      } else {
        return null;
      }
    };
    attrQuery = conditionalURl(attrObj, spliter);

    if (attrQuery.length) {
      return attrObj.condition + "(" + attrQuery + ")";
    } else {
      return null;
    }
  },

  extractUrl: (options) => {
    let attrObj = {};
    let urlObj = options.value;
    let formatDate = options.formatDate;
    let spliter = 1;
    let apiObj = options.apiObj;
    const mapUiOperatorToAPI = (oper) => {
      return queryBuilderUIOperatorToAPI[oper] || oper;
    };
    const createObject = (urlObj) => {
      let finalObj = {};
      finalObj["condition"] = /^AND\(/.test(urlObj) ? "AND" : "OR";
      urlObj =
        finalObj.condition === "AND"
          ? urlObj.substr(4).slice(0, -1)
          : urlObj.substr(3).slice(0, -1);
      finalObj[apiObj ? "criterion" : "rules"] = urlObj
        .split("|" + spliter + "|")
        .map((obj) => {
          let isStringNested = obj.split("|" + (spliter + 1) + "|").length > 1;
          let isCondition = /^AND\(/.test(obj) || /^OR\(/.test(obj);
          if (isStringNested && isCondition) {
            ++spliter;
            return createObject(obj);
          } else if (isCondition) {
            return createObject(obj);
          } else {
            let temp = obj.split("::") || obj.split("|" + spliter + "|");
            let rule = {};
            if (apiObj) {
              rule = {
                attributeName: temp[0],
                operator: mapUiOperatorToAPI(temp[1]),
                attributeValue: temp[2]?.trim()
              };
              rule.attributeValue =
                rule.type === "date" && formatDate && rule.attributeValue.length
                  ? formatedDate({
                      date: parseInt(rule.attributeValue),
                      zone: false
                    })
                  : rule.attributeValue;
            } else {
              rule = {
                id: temp[0],
                operator: temp[1],
                value: temp[2]?.trim()
              };
              if (temp[3]) {
                rule["type"] = temp[3];
              }
              if (rule.operator === "TIME_RANGE") {
                if (temp[2].indexOf(",") > -1) {
                  rule.value = temp[2]
                    .split(",")
                    .map(function (udKey) {
                      return formatedDate({
                        date: parseInt(udKey?.trim()),
                        zone: false
                      });
                    })
                    .join(" - ");
                } else {
                  rule.value =
                    queryBuilderDateRangeAPIValueToUI[rule.value?.trim()] ||
                    rule.value;
                }
              } else if (
                (rule.type === "date" || rule.attributeName == "createTime") &&
                formatDate &&
                rule.value.length
              ) {
                rule.value = formatedDate({
                  date: parseInt(rule.value),
                  zone: false
                });
              }
            }
            return rule;
          }
        });

      return finalObj;
    };
    if (urlObj && urlObj.length) {
      attrObj = createObject(urlObj);
    } else {
      return null;
    }
    return attrObj;
  },
  generateAPIObj: (url) => {
    if (url && url.length) {
      return attributeFilter.extractUrl({ value: url, apiObj: true });
    } else {
      return null;
    }
  }
};

export const generateObjectForSaveSearchApi = (options) => {
  const obj = {
    name: options.name,
    guid: options.guid
  };

  const value = options.value;
  if (value) {
    Object.entries(extractFromUrlForSearch).forEach(([skey, svalue]) => {
      if (typeof svalue === "object" && !Array.isArray(svalue)) {
        Object.entries(svalue).forEach(([k, v]) => {
          let val = value[k];

          if (val !== undefined && val !== null) {
            if (k === "attributes") {
              val = val.split(",");
            } else if (
              ["tagFilters", "entityFilters", "relationshipFilters"].includes(k)
            ) {
              val = attributeFilter.generateAPIObj(val);
            } else if (["includeDE", "excludeST", "excludeSC"].includes(k)) {
              val = val ? false : true;
            }
          }

          if (["includeDE", "excludeST", "excludeSC"].includes(k)) {
            val = val === undefined || val === null ? true : val;
          }

          if (!obj[skey]) {
            obj[skey] = {};
          }
          obj[skey][v] = val;
        });
      } else {
        obj[skey] = value[skey];
      }
    });

    return obj;
  }
};
