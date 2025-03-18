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

import { dateRangesMap, regex, systemAttributes } from "@utils/Enum";
import { dateTimeFormat } from "@utils/Global";
import { cloneDeep } from "@utils/Helper";
import { isEmpty } from "@utils/Utils";
import moment from "moment";
import type { Field, RuleType } from "react-querybuilder";
import { toFullOption } from "react-querybuilder";

export const validator = (r: RuleType) => !!r.value;
let defaultRange = "Last 7 Days";
const getDateConfig = (ruleObj, name, operator) => {
  let valueObj = ruleObj
    ? ruleObj.rules.find((obj) => obj.name == name) || {}
    : {};
  let isTimeRange =
    (valueObj.operator &&
      valueObj.operator === "TIME_RANGE" &&
      operator === "TIME_RANGE") ||
    operator === "TIME_RANGE";
  let obj = {
    opens: "center",
    autoApply: true,
    autoUpdateInput: false,
    timePickerSeconds: true,
    timePicker: true,
    locale: {
      format: dateTimeFormat
    }
    // locale: {
    //   format: "MM/DD/YYYY hh:mm:ss A"
    // }
  };

  if (isTimeRange) {
    let defaultRangeDate = dateRangesMap[defaultRange];
    obj.startDate = defaultRangeDate[0];
    obj.endDate = defaultRangeDate[1];
    obj.singleDatePicker = false;
    obj.ranges = dateRangesMap;
  } else {
    obj.singleDatePicker = true;
    obj.startDate = moment();
    obj.endDate = obj.startDate;
  }

  if (!isEmpty(valueObj) && operator === valueObj.operator) {
    if (isTimeRange) {
      if (valueObj.value.indexOf("-") > -1) {
        let dates = valueObj.value.split("-");
        obj.startDate = dates[0].trim();
        obj.endDate = dates[1].trim();
      } else {
        let dates = dateRangesMap[valueObj.value];
        obj.startDate = dates[0];
        obj.endDate = dates[1];
      }
      obj.singleDatePicker = false;
    } else {
      obj.startDate = moment(Date.parse(valueObj.value));
      obj.endDate = obj.startDate;
      obj.singleDatePicker = true;
    }
  }

  return obj;
};

const isPrimitive = (type) => {
  if (
    type === "int" ||
    type === "byte" ||
    type === "short" ||
    type === "long" ||
    type === "float" ||
    type === "double" ||
    type === "string" ||
    type === "boolean" ||
    type === "date"
  ) {
    return true;
  }
  return false;
};

const getOperator = (type, skipDefault) => {
  var obj = {
    operators: null
  };
  if (type === "string") {
    obj.operators = ["=", "!=", "contains", "begins_with", "ends_with"];
    // obj.operators = obj.operators.concat(["like", "in"]);
  }
  if (type === "date") {
    obj.operators = ["=", "!=", ">", "<", ">=", "<=", "TIME_RANGE"];
  }
  if (
    type === "int" ||
    type === "byte" ||
    type === "short" ||
    type === "long" ||
    type === "float" ||
    type === "double"
  ) {
    obj.operators = ["=", "!=", ">", "<", ">=", "<="];
  }
  if (type === "enum" || type === "boolean") {
    obj.operators = ["=", "!="];
  }
  if (
    (skipDefault === true ||
      skipDefault === false ||
      skipDefault === undefined) &&
    obj.operators
  ) {
    obj.operators = [...obj.operators, ...["is_null", "not_null"]];
  }
  obj.operators = !isEmpty(obj.operators)
    ? obj.operators.map((operator) => {
        if (type == "date" && operator == "TIME_RANGE") {
          return {
            name: operator,
            label: "Time Range"
          };
        }

        return {
          name: operator,
          label: operator
        };
      })
    : [];

  return obj;
};

export const getObjDef = (
  allDataObj?: any,
  attrObj?: any,
  rules?: any,
  isGroup?: any,
  groupType?: any,
  isSystemAttr?: any
): any => {
  const { enums } = allDataObj;
  let getLableWithType = function (label: string, name: string) {
    if (
      name === "__classificationNames" ||
      name === "__customAttributes" ||
      name === "__labels" ||
      name === "__propagatedClassificationNames"
    ) {
      return label;
    } else {
      return label + " (" + attrObj?.typeName + ")";
    }
  };

  let label = !isEmpty(systemAttributes?.[attrObj?.name])
    ? systemAttributes[attrObj?.name]
    : attrObj?.name;
  let obj = {
    id: attrObj?.name,
    name: attrObj?.name,
    label: getLableWithType(label, attrObj?.name),
    plainLabel: label,
    type: attrObj?.typeName,
    inputType: attrObj?.typeName,
    validator
  };

  if (isGroup) {
    obj.group = groupType;
  }
  /* __isIncomplete / IsIncomplete */
  if (
    (isSystemAttr && attrObj?.name === "__isIncomplete") ||
    (isSystemAttr && attrObj?.name === "IsIncomplete")
  ) {
    obj.type = "boolean";
    obj.label =
      (systemAttributes[attrObj?.name]
        ? systemAttributes[attrObj?.name]
        : attrObj?.name.capitalize()) + " (boolean)";

    obj["valueEditorType"] = "select";
    obj["values"] = [
      { name: "true", label: "true" },
      { name: "false", label: "false" }
    ];
    Object.assign(obj, getOperator("boolean"));
  }

  /* Status / __state */
  if (
    (isSystemAttr && attrObj?.name === "Status") ||
    (isSystemAttr && attrObj?.name === "__state") ||
    (isSystemAttr && attrObj?.name === "__entityStatus")
  ) {
    obj.label = systemAttributes[attrObj?.name]
      ? systemAttributes[attrObj?.name]
      : attrObj?.name.capitalize() + " (enum)";
    obj["valueEditorType"] = "select";
    obj["values"] = [
      { name: "ACTIVE", label: "ACTIVE" },
      { name: "DELETED", label: "DELETED" }
    ];
    Object.assign(obj, getOperator("boolean", true));
  }

  // if (obj.inputType === "date") {
  //   obj["valueEditorType"] = "daterangepicker";
  //   obj["values"] = getDateConfig(rules, obj.name);
  //   Object.assign(obj, getOperator(obj.type));
  //   return obj;
  // }

  if (obj.type === "date") {
    obj["inputType"] = "datetime-local";
    obj["values"] = getDateConfig(rules, obj.name);
    Object.assign(obj, getOperator(obj.type));
  }

  if (isPrimitive(obj.type)) {
    if (obj.type === "boolean") {
      obj["valueEditorType"] = "select";
      obj["values"] = [
        { name: "true", label: "true" },
        { name: "false", label: "false" }
      ];
    }
    Object.assign(obj, getOperator(obj.type, false));
    if (regex.RANGE_CHECK.hasOwnProperty(obj.type)) {
      obj.validator = {
        min: regex.RANGE_CHECK[obj.type].min,
        max: regex.RANGE_CHECK[obj.type].max
      };
      if (obj.type === "double" || obj.type === "float") {
        obj.type = "double";
        obj["inputType"] = "number";
      } else if (
        obj.type === "int" ||
        obj.type === "byte" ||
        obj.type === "short" ||
        obj.type === "long"
      ) {
        obj.type = "integer";
        obj["inputType"] = "number";
      }
    }
    return obj;
  }
  let enumObj = !isEmpty(enums)
    ? enums.find((enums) => enums.name == obj.type)
    : {};

  if (enumObj) {
    obj.type = "string";
    obj["valueEditorType"] = "select";
    var value = [];
    if (!isEmpty(enumObj?.elementDefs)) {
      for (let defs of enumObj.elementDefs) {
        value.push({ name: defs.value, label: defs.value });
      }
    }

    obj["values"] = value;
    Object.assign(obj, getOperator("enum"));
    return obj;
  }
};

export const fields = (allDataObj) => {
  const { entitys } = allDataObj;
  let filters = [];
  let entityData = cloneDeep(entitys);

  let entityDef = !isEmpty(entityData)
    ? entityData.find((obj) => obj.name == "__AtlasAuditEntry")
    : {};
  const { attributeDefs = {} } = entityDef || {};
  let auditEntryAttributeDefs = null;
  if (entityDef) {
    auditEntryAttributeDefs = Object.assign({}, attributeDefs) || null;
  }
  if (!isEmpty(auditEntryAttributeDefs)) {
    for (const attributes in auditEntryAttributeDefs) {
      let returnObj: any = getObjDef(
        allDataObj,
        auditEntryAttributeDefs[attributes],
        undefined,
        undefined,
        undefined,
        undefined
      );
      if (returnObj) {
        filters.push(returnObj);
      }
    }
  }

  return (filters satisfies Field[]).map((o) => toFullOption(o));
};
