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

import { IconButton } from "./muiComponents";
import {
  escapeHtml,
  extractKeyValueFromEntity,
  isArray,
  isBoolean,
  isEmpty,
  isNumber,
  isObject,
  isString
} from "../utils/Utils";
import { JSONPrettyPrint, getValue } from "../utils/CommonViewFunction";
import { useSelector } from "react-redux";
import { entityStateReadOnly } from "../utils/Enum";
import { Link, useLocation } from "react-router-dom";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import { useRef, useState } from "react";
import { getDetailPageData } from "../api/apiMethods/detailpageApiMethod";

type EntityRefField = {
  guid?: string;
  name?: string;
  status?: string;
  entityStatus?: string;
  state?: string;
  id?: { state?: string };
  typeName?: string;
  attributes?: Record<string, unknown>;
  values?: Record<string, unknown>;
};

type ExtractObjectProps = {
  keyValue?: EntityRefField | EntityRefField[] | string | boolean | number | unknown[];
  properties?: string;
  skipHeaderFetch?: boolean;
};

type TypeHeaderEntry = {
  name: string;
  category?: string;
};

export const ExtractObject = (props: ExtractObjectProps) => {
  const { skipHeaderFetch = false } = props;
  const { typeHeaderData } = useSelector(
    (state: { typeHeader: { typeHeaderData?: TypeHeaderEntry[] } }) =>
      state.typeHeader
  );
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const [headerData, setHeaderData] = useState<string>("");
  const fetchedGuidsRef = useRef<Set<string>>(new Set());

  const getGuid = (guid: string) => {
    if (skipHeaderFetch || isEmpty(guid) || fetchedGuidsRef.current.has(guid)) {
      return;
    }
    fetchedGuidsRef.current.add(guid);
    getInputOutputValue(guid);
  };

  const getInputOutputValue = async (guid: string) => {
    if (!isEmpty(guid)) {
      try {
        const { data: response } = await getDetailPageData(guid, {}, "headers");
        const { name } = extractKeyValueFromEntity(response);
        setHeaderData(name as string);
      } catch {
        // Error handled silently
      }
    }
  };

  const valueOfArray: string[] = [];
  let keyValue = props.keyValue;
  let nameVal = "";
  let tempLink = "";
  let deleteIcon = false;
  let fetchVal = false;

  if (!isArray(keyValue) && isObject(keyValue)) {
    keyValue = [keyValue as EntityRefField];
  }

  const keyValueArray = (keyValue ?? []) as EntityRefField[];

  for (let i = 0; i < keyValueArray.length; i++) {
    const inputOutputField = keyValueArray[i];
    const id = inputOutputField.guid;
    const entityId = inputOutputField.id;
    const idState = isObject(entityId) ? entityId?.state : undefined;
    const status =
      inputOutputField.status ||
      inputOutputField.entityStatus ||
      idState ||
      inputOutputField.state;
    const readOnly = entityStateReadOnly[status as keyof typeof entityStateReadOnly];

    if (!inputOutputField.attributes && inputOutputField.values) {
      inputOutputField.attributes = inputOutputField.values;
    }
    if (
      isString(inputOutputField) ||
      isBoolean(inputOutputField) ||
      isNumber(inputOutputField)
    ) {
      const tempVarfor$check = String(inputOutputField);
      if (tempVarfor$check.indexOf("$") == -1) {
        const tmpVal = getValue(
          inputOutputField as unknown as string | boolean | number
        );

        valueOfArray.push(
          '<span class="json-string">' + escapeHtml(String(tmpVal)) + "</span>"
        );
      }
    } else if (isObject(inputOutputField) && id == undefined) {
      let attributesList: Record<string, unknown> = inputOutputField;
      if (typeHeaderData && inputOutputField.typeName) {
        const typeNameCategory = typeHeaderData.find(
          (obj) => obj.name == inputOutputField.typeName
        );

        if (
          attributesList?.attributes &&
          typeNameCategory?.category === "STRUCT"
        ) {
          attributesList = attributesList.attributes as Record<string, unknown>;
        }
      }
      valueOfArray.push(JSONPrettyPrint(attributesList) as string);
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
        const keys = Array.from(searchParams.keys());
        for (let j = 0; j < keys.length; j++) {
          if (keys[j] != "searchType") {
            searchParams.delete(keys[j]);
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
                  __html: valueOfArray.join("")
                }}
              ></span>
            </code>
          </pre>
        ) : (
          <span
            dangerouslySetInnerHTML={{
              __html: valueOfArray.join("")
            }}
          ></span>
        )
      ) : (
        <>
          {tempLink != "" ? (
            <>
              <Link
                className={`entity-name nav-link max-100 text-decoration-none ${
                  deleteIcon ? "entity-name-deleted" : "text-blue"
                }`}
                to={{
                  pathname: tempLink,
                  search: searchParams.toString() ? searchParams.toString() : ""
                }}
                style={{
                  display: "inline-block",
                  maxWidth: "100%",
                  textOverflow: "ellipsis",
                  overflow: "hidden",
                  whiteSpace: "nowrap",
                  verticalAlign: "bottom"
                }}
                title={nameVal}
              >
                {nameVal}
              </Link>
              {deleteIcon && (
                <IconButton
                  aria-label="Deleted entity"
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
