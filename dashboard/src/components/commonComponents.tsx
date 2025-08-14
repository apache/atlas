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

import { IconButton } from "../components/muiComponents";

import {
  dateFormat,
  extractKeyValueFromEntity,
  formatedDate,
  isArray,
  isBoolean,
  isEmpty,
  isFunction,
  isNumber,
  isObject,
  isString
} from "../utils/Utils";
import { JSONPrettyPrint, getValue } from "../utils/CommonViewFunction";
import { useSelector } from "react-redux";
import {
  entityStateReadOnly,
  filterQueryValue,
  queryBuilderDateRangeUIValueToAPI,
  systemAttributes
} from "../utils/Enum";
import { Link, useLocation, useNavigate } from "react-router-dom";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import { useEffect, useState } from "react";
import { getDetailPageData } from "../api/apiMethods/detailpageApiMethod";
import moment from "moment";

export const EllipsisText = (props: any) => {
  const { children } = props;

  return <div className="chip-ellipsis cursor-pointer">{children}</div>;
};

export const ExtractObject = (props: any) => {
  const { typeHeaderData }: any = useSelector((state: any) => state.typeHeader);
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const [headerData, setHeaderData] = useState<string>("");
  let triggerHeaderApi = "";

  useEffect(() => {
    getInputOutputValue(triggerHeaderApi);
  }, [triggerHeaderApi]);

  const getGuid = (guid: string) => {
    if (!isEmpty(guid)) {
      triggerHeaderApi = guid;
    }
  };

  const getInputOutputValue = async (guid: string) => {
    if (!isEmpty(guid)) {
      try {
        const { data: response } = await getDetailPageData(guid, {}, "headers");
        const { name } = extractKeyValueFromEntity(response);
        setHeaderData(name as string);
      } catch (error) {
        console.log(error);
      }
    }
  };

  let valueOfArray = [],
    keyValue = props.keyValue,
    nameVal = "",
    tempLink = "",
    deleteIcon = false,
    fetchVal = false;

  if (!isArray(keyValue) && isObject(keyValue)) {
    keyValue = [keyValue];
  }
  for (var i = 0; i < keyValue?.length; i++) {
    let inputOutputField = keyValue[i],
      id = inputOutputField.guid,
      status =
        inputOutputField.status ||
        inputOutputField.entityStatus ||
        (isObject(inputOutputField.id)
          ? inputOutputField.id.state
          : inputOutputField.state),
      readOnly = entityStateReadOnly[status];

    if (!inputOutputField.attributes && inputOutputField.values) {
      inputOutputField["attributes"] = inputOutputField.values;
    }
    if (
      isString(inputOutputField) ||
      isBoolean(inputOutputField) ||
      isNumber(inputOutputField)
    ) {
      let tempVarfor$check = inputOutputField.toString();
      if (tempVarfor$check.indexOf("$") == -1) {
        let tmpVal = getValue(inputOutputField as any);

        valueOfArray.push('<span class="json-string">' + tmpVal + "</span>");
      }
    } else if (isObject(inputOutputField) && id == undefined) {
      let attributesList = inputOutputField;
      if (typeHeaderData && inputOutputField.typeName) {
        let typeNameCategory = typeHeaderData.find(
          (obj: { name: string }) => obj.name == inputOutputField.typeName
        );

        if (
          attributesList?.attributes &&
          typeNameCategory?.category === "STRUCT"
        ) {
          attributesList = attributesList.attributes;
        }
      }
      valueOfArray.push(JSONPrettyPrint(attributesList));
    }

    if (id && inputOutputField) {
      const { name }: { name: string } = extractKeyValueFromEntity(
        inputOutputField,
        "",
        "",
        getGuid,
        headerData
      );
      nameVal = name;
      if (inputOutputField.typeName == "AtlasGlossaryTerm") {
        tempLink = `/glossary/${id}`;
        let keys = Array.from(searchParams.keys());
        for (let i = 0; i < keys.length; i++) {
          if (keys[i] != "searchType") {
            searchParams.delete(keys[i]);
          }
        }
        searchParams.set("guid", id);
        searchParams.set("gtype", "term");
        searchParams.set("viewType", "term");
        fetchVal = true;
      } else {
        tempLink = `/detailPage/${id}`;
      }
    }

    if (readOnly) {
      if (!fetchVal) {
        deleteIcon = true;
      } else {
        fetchVal = false;
      }
    }
  }

  return (
    <>
      {valueOfArray?.length > 0 ? (
        props.properties != undefined ? (
          <pre className="code-block fixed-height">
            <code>
              <span
                dangerouslySetInnerHTML={{
                  __html: valueOfArray as any
                }}
              ></span>
            </code>
          </pre>
        ) : (
          <span
            dangerouslySetInnerHTML={{
              __html: valueOfArray as any
            }}
          ></span>
        )
      ) : (
        <>
          {tempLink != "" ? (
            <>
              <Link
                className="entity-name nav-link max-100 text-blue text-decoration-none"
                to={{
                  pathname: tempLink,
                  search: searchParams.toString() ? searchParams.toString() : ""
                }}
                style={{
                  width: "unset !important",
                  whiteSpace: "normal",
                  color: deleteIcon ? "#bb5838" : "#4a90e2"
                }}
              >
                {nameVal}
              </Link>
              {deleteIcon && (
                <IconButton
                  aria-label="back"
                  sx={{
                    display: "inline-flex",
                    position: "relative",
                    padding: "0",
                    marginLeft: "4px"
                  }}
                >
                  <DeleteOutlineOutlinedIcon
                    className="delete-icon"
                    sx={{ fontSize: "1.25rem", height: "24px" }}
                  />
                </IconButton>
              )}
            </>
          ) : (
            "N/A"
          )}
        </>
      )}
    </>
  );
};

export const GetArrayValue = ({
  values,
  properties,
  referredEntities
}: {
  values: string[];
  properties?: string;
  referredEntities?: any;
}) => {
  return !isEmpty(values) ? (
    properties != undefined ? (
      <pre className="code-block fixed-height shrink">
        <code>
          {values.map((obj: any, i: number) => {
            if (isObject(obj)) {
              return (
                <>
                  <ExtractObject
                    keyValue={
                      referredEntities?.[obj?.guid] != undefined
                        ? referredEntities[obj.guid]
                        : obj
                    }
                    properties={properties}
                  />
                  <br />
                </>
              );
            }
            return (
              <span className="json-string">
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
          <span className="json-string">
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
  keys?: string
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

  if (
    keyValue == "profileData" ||
    (keyValue == "isIncomplete" && keyValue == false)
  ) {
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
      return (
        <ExtractObject keyValue={filteredValues} properties={properties} />
      );
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
        />
      );
    }
  } else {
    if (!isEmpty(keyValue) && isObject(keyValue) && !isArray(keyValue)) {
      let filteredValues =
        filterEntityData?.relationshipAttributes?.[keys as string] != undefined
          ? filterEntityData.relationshipAttributes[keys as string]
          : keyValue;
      return (
        <ExtractObject keyValue={filteredValues} properties={properties} />
      );
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
