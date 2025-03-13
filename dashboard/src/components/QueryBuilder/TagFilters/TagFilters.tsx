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
  AccordionDetails,
  AccordionSummary,
  Accordion,
  Typography
} from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import { attributeFilter } from "@utils/CommonViewFunction";
import { addOnClassification } from "@utils/Enum";
import { getNestedSuperTypeObj, isEmpty } from "@utils/Utils";
import { getObjDef } from "@views/Administrator/Audits/AuditsFilter/AuditFiltersFields";
import QueryBuilder, { Field, toFullOption } from "react-querybuilder";
import { useLocation } from "react-router-dom";
import { TagCustomValueEditor } from "./TagCustomValueEditor";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";

const TagFilters = ({
  allDataObj,
  classificationQuery,
  setClassificationQuery
}: any) => {
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const tagParams = searchParams.get("tag");
  const { classificationData }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { rootClassificationTypeData }: any = useAppSelector(
    (state: any) => state.rootClassification
  );
  const { rootClassificationData = {} } = rootClassificationTypeData || {};
  const { classificationDefs } = classificationData;
  const paramsObject: Record<string, any> = {};
  searchParams.forEach((value: any, key: string | number) => {
    paramsObject[key] = value;
  });

  const { type, tag } = paramsObject || {};
  // let tagAttributeLength;
  let attrTagObj =
    !isEmpty(classificationDefs) && !isEmpty(tag)
      ? classificationDefs.find((tag: { name: string }) => {
          return tag.name == tagParams;
        })
      : {};

  if (attrTagObj) {
    attrTagObj = getNestedSuperTypeObj({
      data: attrTagObj,
      collection: classificationDefs,
      attrMerge: true
    });
    // tagAttributeLength = attrTagObj.length;
  }

  const fields = () => {
    let filters = [];
    let isGroupView = true;
    let rules_widgets = null;
    let systemAttrArr;

    if (addOnClassification[0]) {
      systemAttrArr = rootClassificationData?.attributeDefs;
      // tagAttributeLength = systemAttrArr.length;
    }

    if (!isEmpty(paramsObject)) {
      rules_widgets = attributeFilter.extractUrl({
        value: undefined,
        formatDate: true
      });
    }
    for (const attrObj in attrTagObj) {
      let returnObj: any = getObjDef(
        allDataObj,
        attrTagObj[attrObj],
        rules_widgets,
        isGroupView,
        tag + " Attribute"
        // "Sub Attribute"
      );
      if (returnObj) {
        filters.push(returnObj);
      }
    }

    var sortMap: Record<string, number> = {
      __guid: 1,
      __typeName: 2,
      __timestamp: 3,
      __modificationTimestamp: 4,
      __createdBy: 5,
      __modifiedBy: 6,
      __isIncomplete: 7,
      __classificationNames: 9,
      __propagatedClassificationNames: 10,
      __labels: 11,
      __customAttributes: 12
    };
    if (type) {
      sortMap["__state"] = 8;
    } else {
      sortMap["__entityStatus"] = 8;
    }

    for (const attrObj in systemAttrArr) {
      let returnObj: any = getObjDef(
        allDataObj,
        systemAttrArr[attrObj],
        rules_widgets,
        isGroupView,
        // tag + " Attribute"
        "System Attribute"
      );
      if (returnObj) {
        filters.push(returnObj);
      }
    }

    return (filters satisfies Field[]).map((o) => toFullOption(o));
  };

  const groupedFields =
    fields()?.reduce(
      (
        acc: { [x: string]: any[] },
        field: { group: string | number | undefined }
      ) => {
        if (field && field.group !== undefined) {
          if (!acc[field.group]) {
            acc[field.group] = [];
          }
          acc[field.group].push(field);
        }
        return acc;
      },
      {}
    ) || {};

  const fieldsObj = Object.keys(groupedFields).map((group) => ({
    label: group,
    options: groupedFields[group]
  }));
  return (
    <Accordion
      defaultExpanded
      sx={{ borderBottom: "1px solid rgba(0, 0, 0, 0.12) !important" }}
    >
      <AccordionSummary aria-controls="panel1-content" id="panel1-header">
        <Typography className="text-color-green" fontWeight="600">
          Classification: {tagParams}
        </Typography>
      </AccordionSummary>
      <AccordionDetails>
        <QueryBuilder
          fields={fieldsObj}
          controlClassnames={{ queryBuilder: "queryBuilder-branches" }}
          query={classificationQuery}
          onQueryChange={setClassificationQuery}
          controlElements={{ valueEditor: TagCustomValueEditor }}
          translations={{
            addGroup: {
              label: (
                <>
                  <AddOutlinedIcon fontSize="small" /> Add filter group
                </>
              )
            },
            addRule: {
              label: (
                <>
                  <AddOutlinedIcon fontSize="small" /> Add filter
                </>
              )
            }
          }}
        />
      </AccordionDetails>
    </Accordion>
  );
};

export default TagFilters;
