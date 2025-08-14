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
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Typography
} from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import { getNestedSuperTypeObj, isEmpty } from "@utils/Utils";
import { getObjDef } from "@views/Administrator/Audits/AuditsFilter/AuditFiltersFields";
import QueryBuilder, {
  Field,
  toFullOption,
  ValueEditor
} from "react-querybuilder";
import { useLocation } from "react-router-dom";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";

const RelationshipFilters = ({
  allDataObj,
  relationshipQuery,
  setRelationshipQuery
}: any) => {
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const relationshipParams = searchParams.get("relationshipName");
  const { relationships }: any = useAppSelector(
    (state: any) => state.relationships
  );
  const { relationshipDefs } = relationships || {};

  let attrTagObj =
    !isEmpty(relationshipDefs) && !isEmpty(relationshipParams)
      ? relationshipDefs.find((relationship: { name: string }) => {
          return relationship.name == relationshipParams;
        })
      : {};

  if (attrTagObj) {
    // let realtionshipAttributeLength;
    attrTagObj = getNestedSuperTypeObj({
      data: attrTagObj,
      collection: relationshipDefs,
      attrMerge: true
    });
    // realtionshipAttributeLength = attrTagObj.length;
  }

  const fields = () => {
    let filters = [];
    let isGroupView = true;
    let rules_widgets = null;

    for (const attrObj in attrTagObj) {
      let returnObj: any = getObjDef(
        allDataObj,
        attrTagObj[attrObj],
        rules_widgets,
        isGroupView,
        relationshipParams + " Attribute"
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
    <Accordion defaultExpanded>
      <AccordionSummary aria-controls="panel1-content" id="panel1-header">
        <Typography className="text-color-green" fontWeight="600">
          Relationship: {relationshipParams}
        </Typography>
      </AccordionSummary>
      <AccordionDetails>
        {!isEmpty(fieldsObj) ? (
          <QueryBuilder
            fields={fieldsObj}
            controlClassnames={{ queryBuilder: "queryBuilder-branches" }}
            query={relationshipQuery}
            onQueryChange={setRelationshipQuery}
            controlElements={{
              valueEditor: (props) => {
                if (
                  props.operator === "is_null" ||
                  props.operator === "not_null"
                ) {
                  return;
                }
                return <ValueEditor {...props} />;
              }
            }}
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
        ) : (
          <Typography
            textAlign="center"
            fontWeight="600"
            color="text.secondary"
          >
            No Attributes are available !
          </Typography>
        )}
      </AccordionDetails>
    </Accordion>
  );
};

export default RelationshipFilters;
