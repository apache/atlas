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

import { useAppSelector } from "@hooks/reducerHook";
import {
  Divider,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography
} from "@mui/material";
import { cloneDeep } from "@utils/Helper";
import {
  customSortBy,
  getNestedSuperTypeObj,
  isEmpty,
  isNull,
  isObject,
  isString
} from "@utils/Utils";

const AttributeTable = ({ values }: any) => {
  let classification = { ...values };
  const { attributes, typeName } = classification;
  const { classificationData }: any = useAppSelector(
    (state: any) => state.classification
  );

  const allClassificationData = cloneDeep(classificationData);

  const { classificationDefs } = allClassificationData;

  const classificationObj = !isEmpty(typeName)
    ? classificationDefs.find((obj: { name: string }) => obj.name == typeName)
    : {};
  let attributeDefList = !isEmpty(classificationObj)
    ? getNestedSuperTypeObj({
        data: classificationObj,
        collection: classificationDefs,
        attrMerge: true
      })
    : null;
  let sortedObj = !isEmpty(attributeDefList)
    ? customSortBy(
        attributeDefList?.map((obj: any) => {
          obj["sortKey"] =
            obj.name && isString(obj.name) ? obj.name.toLowerCase() : "-";
          return obj;
        }),
        ["sortKey"]
      )
    : [];
  const getValues = (value: any) => {
    let val = isNull(attributes?.[value.name]) ? "-" : attributes?.[value.name];

    if (value.typeName == "boolean") {
      val = val == true ? "true" : "false";
    }
    if (isObject(val)) {
      val = JSON.stringify(val);
    }

    return val;
  };

  return !isEmpty(sortedObj) ? (
    <TableContainer
      component={Paper}
      elevation={0}
      className="classification-table-container"
    >
      <Table size="small" aria-label="a dense table">
        <TableHead>
          <TableRow>
            <TableCell width="50%">
              <Typography fontWeight={600}>Name</Typography>
            </TableCell>
            <Divider
              orientation="vertical"
              className="classification-table-divider"
              flexItem={true}
            />
            <TableCell>
              <Typography fontWeight={600}>value</Typography>
            </TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {sortedObj.map((obj: any) => {
            let key = obj.name;
            return (
              <TableRow key={key}>
                <TableCell component="th" scope="row" width="50%">
                  {key}
                </TableCell>
                <Divider
                  orientation="vertical"
                  flexItem={true}
                  className="classification-table-divider"
                />
                <TableCell component="th" scope="row" align="left">
                  {getValues(obj)}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </TableContainer>
  ) : (
    <Typography>NA</Typography>
  );
};

export default AttributeTable;
