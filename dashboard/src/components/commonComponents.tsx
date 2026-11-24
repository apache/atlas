//@ts-nocheck

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
  dateFormat,
  formatedDate,
  isArray,
  isBoolean,
  isEmpty,
  isFunction,
  isNumber,
  isObject,
  isString
} from "../utils/Utils";
import moment from "moment";
import { renderEntityRefValue } from "./entityRefRenderers";

export { ExtractObject } from "./ExtractObject";
export { default as AuditEntityRefLink } from "./AuditEntityRefLink";

export const EllipsisText = (props: any) => {
  const { children } = props;

  return <div className="chip-ellipsis cursor-pointer">{children}</div>;
};

export const GetArrayValue = ({
  values,
  properties,
  referredEntities,
  auditDetails
}: {
  values: string[];
  properties?: string;
  referredEntities?: any;
  auditDetails?: boolean;
}) => {
  return !isEmpty(values) ? (
    properties != undefined ? (
      <pre className="code-block fixed-height shrink">
        <code>
          {values.map((obj: any, i: number) => {
            if (isObject(obj)) {
              return (
                <span key={obj?.guid ?? i}>
                  {renderEntityRefValue({
                    entityRef: obj,
                    referredEntities,
                    properties,
                    auditDetails
                  })}
                  <br />
                </span>
              );
            }
            return (
              <span key={i} className="json-string">
                {obj}
                {i < values?.length - 1 && ", "}
              </span>
            );
          })}
        </code>
      </pre>
    ) : (
      values.map((obj: any, i: number) => {
        return (
          <span key={i} className="json-string">
            {obj}
            {i < values?.length - 1 && ", "}
          </span>
        );
      })
    )
  ) : (
    <span>NA</span>
  );
};

export const getValues = (
  values: any,
  entityData: any,
  entity: any,
  relationShipAttr?: string,
  properties?: string,
  referredEntities?: any,
  filterEntityData?: any,
  keys?: string,
  auditDetails?: boolean
) => {
  var tempObj = {
    attributeDefs: [entity],
    valueObject: {
      [entity?.name]:
        relationShipAttr != undefined
          ? entityData
          : properties !== undefined
            ? values
            : values?.row?.original?.attributes[entity?.name]
    }
  };
  let keyValue = tempObj.valueObject[entity?.name];
  let currentValue = properties !== undefined ? values : values.getValue();
  let attributeTypeName: string = !isEmpty(entity?.attributeDefs)
    ? entity?.attributeDefs?.find((obj: { name: string }) => obj.name == keys)
      ?.typeName
    : "";

  // Filter out profileData, but show isIncomplete (matching Classic UI behavior)
  if (keyValue == "profileData") {
    return;
  }

  if (entity?.typeName || entityData?.typeName) {
    if (
      (entity?.typeName || entityData?.typeName) == "date" ||
      keys == "createTime" ||
      keys == "modifiedTime" ||
      keys == "updateTime" ||
      keys == "startTime" ||
      keys == "endTime"
    ) {
      return (
        keyValue !== undefined && (
          <span>
            {moment(keyValue).isValid()
              ? formatedDate({
                date: keyValue
                // zone: false,
                // dateFormat: dateFormat
              })
              : "N/A"}
          </span>
        )
      );
    }
    if (!isEmpty(keyValue) && isObject(keyValue) && !isArray(keyValue)) {
      let filteredValues =
        filterEntityData?.relationshipAttributes?.[keys as string] != undefined
          ? filterEntityData.relationshipAttributes[keys as string]
          : keyValue;
      return renderEntityRefValue({
        entityRef: filteredValues,
        referredEntities,
        properties,
        auditDetails
      });
    }
    if (!isEmpty(currentValue) && isArray(currentValue)) {
      let filteredValues =
        filterEntityData?.relationshipAttributes?.[keys as string] != undefined
          ? filterEntityData.relationshipAttributes[keys as string]
          : properties !== undefined
            ? values
            : values.getValue();

      return (
        <GetArrayValue
          values={filteredValues}
          properties={properties}
          referredEntities={referredEntities}
          auditDetails={auditDetails}
        />
      );
    }
  } else {
    if (!isEmpty(keyValue) && isObject(keyValue) && !isArray(keyValue)) {
      let filteredValues =
        filterEntityData?.relationshipAttributes?.[keys as string] != undefined
          ? filterEntityData.relationshipAttributes[keys as string]
          : keyValue;
      return renderEntityRefValue({
        entityRef: filteredValues,
        referredEntities,
        properties,
        auditDetails
      });
    }
  }
  if (!isEmpty(currentValue) && isArray(currentValue)) {
    let filteredValues =
      filterEntityData?.relationshipAttributes?.[keys as string] != undefined
        ? filterEntityData.relationshipAttributes[keys as string]
        : properties !== undefined
          ? values
          : values.getValue();
    return (
      <GetArrayValue
        values={filteredValues}
        properties={properties}
        referredEntities={referredEntities}
        auditDetails={auditDetails}
      />
    );
  }
  if (
    !isEmpty(currentValue) && properties !== undefined
      ? isBoolean(values)
      : isBoolean(isFunction(values) ? values.getValue() : undefined)
  ) {
    let currentVal = currentValue;
    return <span>{currentVal == false ? "false" : "true"}</span>;
  }
  if (
    !isEmpty(currentValue) && properties !== undefined
      ? isString(values)
      : isString(isFunction(values) ? values.getValue() : undefined)
  ) {
    return (
      <span>
        {!isEmpty(currentValue)
          ? properties !== undefined
            ? values
            : values.getValue()
          : "N/A"}
      </span>
    );
  }
  if (
    !isEmpty(currentValue) &&
    (properties !== undefined
      ? isNumber(values)
      : isNumber(isFunction(values) ? values.getValue() : undefined)) &&
    ((attributeTypeName || entity?.typeName || entityData?.typeName) ==
      "date" ||
      keys == "createTime" ||
      keys == "modifiedTime" ||
      keys == "updateTime" ||
      keys == "startTime" ||
      keys == "endTime")
  ) {
    return (
      currentValue !== undefined && (
        <span>
          {moment(keyValue).isValid()
            ? formatedDate({
              date: currentValue,
              zone: false,
              dateFormat: dateFormat
            })
            : "N/A"}
        </span>
      )
    );
  }

  return (
    <span>
      {!isEmpty(currentValue)
        ? properties !== undefined
          ? values
          : values.getValue()
        : "N/A"}
    </span>
  );
};

export const GetNumberSuffix = (options: { number: any; sup?: boolean }) => {
  if (options && options.number) {
    let n = options.number;
    let s = ["th", "st", "nd", "rd"];
    let v = n % 100;
    let suffix = s[(v - 20) % 10] || s[v] || s[0];
    if (options.sup) {
      return (
        <>
          <>{n}</>
          <sup>{suffix}</sup>
        </>
      );
    } else {
      return (
        <>
          {n}
          {suffix}
        </>
      );
    }
  }
};
