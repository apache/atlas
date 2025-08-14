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

import { Popover, Stack, Typography } from "@mui/material";
import { SetStateAction, useEffect, useState } from "react";
import type {
  Operator,
  RuleGroupType,
  ValueEditorProps
} from "react-querybuilder";
import {
  defaultOperators,
  QueryBuilder,
  ValueEditor
} from "react-querybuilder";
import "react-querybuilder/dist/query-builder.scss";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  CustomButton
} from "@components/muiComponents";
import { fields } from "./AuditFiltersFields";
import { useAppSelector } from "@hooks/reducerHook";
import DatePicker from "react-datepicker";
import moment from "moment";
import { GlobalQueryState, isEmpty } from "@utils/Utils";
import { attributeFilter } from "@utils/CommonViewFunction";
import { cloneDeep } from "@utils/Helper";
import { timeRangeOptions } from "@utils/Enum";
import CustomDatepicker from "@components/DatePicker/CustomDatePicker";

const customOperators: Operator[] = [
  ...defaultOperators,
  { name: "TIME_RANGE", label: "Time Range" }
];

const CustomValueEditor: React.FC<ValueEditorProps> = (props) => {
  const { field, operator, value, handleOnChange, loader } = props;
  const [_customRange, setCustomRange] = useState<[Date | null, Date | null]>([
    null,
    null
  ]);
  const [showDatePicker, setShowDatePicker] = useState(false);
  const [dateRange, setDateRange] = useState<any>([null, null]);
  const [selectedDateValue, setSelectedDateValue] = useState(
    moment(props.value).isValid() ? moment(props.value) : null
  );
  const [startDate, endDate] = dateRange;

  const handleTimeRangeChange = (selectedValue: string) => {
    handleOnChange(selectedValue);
    if (selectedValue === "CUSTOM_RANGE") {
      setShowDatePicker(true);
    } else {
      setShowDatePicker(false);
      setCustomRange([null, null]);
    }
  };

  if (
    (field === "startTime" || field == "endTime") &&
    operator === "TIME_RANGE"
  ) {
    return (
      <div>
        <select
          className="rule-operators"
          value={value}
          onChange={(e) => handleTimeRangeChange(e.target.value)}
        >
          <option value="">Select Time Range</option>
          {timeRangeOptions.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        {showDatePicker && (
          <CustomDatepicker
            selectsRange
            timeIntervals={1}
            timeFormat="hh:mm aa"
            timeCaption="Time"
            shoowTimeInput
            showPopperArrow={false}
            popperProps={{ strategy: "fixed" }}
            startDate={
              moment(startDate).isValid()
                ? moment(startDate).toDate()
                : undefined
            }
            endDate={
              moment(endDate).isValid() ? moment(endDate).toDate() : undefined
            }
            onChange={(update: [Date | null, Date | null] | null) => {
              const safeUpdate: [Date | null, Date | null] = update ?? [
                null,
                null
              ];
              setDateRange(safeUpdate);
              if (safeUpdate[0] && safeUpdate[1]) {
                const startEpoch = moment(safeUpdate[0]).valueOf();
                const endEpoch = moment(safeUpdate[1]).valueOf();
                props.handleOnChange(`${startEpoch},${endEpoch}`);
              } else {
                props.handleOnChange("");
              }
            }}
            selected={undefined}
          />
        )}
      </div>
    );
  }

  if (props.inputType == "datetime-local") {
    return (
      <CustomDatepicker
        timeIntervals={1}
        timeFormat="hh:mm aa"
        timeCaption="Time"
        showPopperArrow={false}
        popperProps={{ strategy: "fixed" }}
        selected={
          selectedDateValue && moment(selectedDateValue).isValid()
            ? moment(selectedDateValue).toDate()
            : moment().toDate()
        }
        onChange={(date: Date | null) => {
          const value = date ? moment(date) : moment();
          setSelectedDateValue(value);
          props.handleOnChange(value.valueOf());
        }}
        dateFormat="MM/dd/yyyy h:mm:ss aa"
        showTimeInput
      />
    );
  }
  if (operator === "is_null" || operator === "not_null") {
    return;
  }
  return <ValueEditor {...props} />;
};

const AuditFilters = ({
  popoverId,
  filtersOpen,
  filtersPopover,
  handleCloseFilterPopover,
  setupdateTable,
  queryApiObj,
  setQueryApiObj
}: any) => {
  const { entityData = {} } = useAppSelector((state: any) => state.entity);
  const { classificationData }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { enumObj = {} }: any = useAppSelector((state: any) => state.enum);
  const { classificationDefs } = classificationData || {};
  const { entityDefs } = entityData;
  const { enumDefs = {} }: any = enumObj?.data || {};
  const initialQuery: RuleGroupType = {
    combinator: "and",
    rules: []
  };

  const [queryBuilder, setQueryBuilder] = useState(
    isEmpty(GlobalQueryState.getQuery())
      ? initialQuery
      : GlobalQueryState.getQuery()
  );

  let allDataObj = {
    entitys: entityDefs,
    enums: enumDefs,
    tags: classificationDefs
  };

  function processCombinators(obj: {
    [x: string]: any;
    combinator: string;
    rules: any[];
  }) {
    if (obj.combinator) {
      obj["condition"] = obj.combinator.toUpperCase();
      delete obj["combinator"];
    }

    if (Array.isArray(obj.rules)) {
      obj.rules.forEach((rule: any) => {
        processCombinators(rule);
      });
    }
    return obj;
  }

  const handleFilterClick = () => {
    let isFilterValidate = true;
    let ruleUrl;
    if (!isEmpty(queryBuilder)) {
      let queryBuilderData = cloneDeep(queryBuilder);
      ruleUrl = attributeFilter.generateUrl({
        value: processCombinators(queryBuilderData),
        formatedDateToLong: true
      });
    } else {
      isFilterValidate = false;
    }

    let auditFilters = attributeFilter.generateAPIObj(ruleUrl);
    if (isFilterValidate) {
      setQueryApiObj(auditFilters);
      GlobalQueryState.setQuery(queryBuilder);
      setupdateTable(moment.now());
    }
  };

  const handleQueryChange = (newQuery: SetStateAction<{}>) => {
    const enrichedRules = newQuery.rules.map((rule: { field: any }) => {
      const field = fields(entityDefs, enumDefs).find(
        (f) => f.name === rule.field
      );
      if (field) {
        return {
          ...rule,
          type: field.type
        };
      }
      return rule;
    });

    setQueryBuilder({ ...newQuery, rules: enrichedRules });
    GlobalQueryState.setQuery(newQuery);
  };

  return (
    <>
      {
        <Popover
          id={popoverId}
          open={filtersOpen}
          anchorEl={filtersPopover}
          onClose={handleCloseFilterPopover}
          anchorOrigin={{
            vertical: "bottom",
            horizontal: "left"
          }}
          transformOrigin={{
            vertical: "top",
            horizontal: "left"
          }}
          sx={{
            "& .MuiPaper-root": {
              transitionDelay: "300ms !important"
            }
          }}
        >
          <Stack width="700px">
            <Accordion defaultExpanded>
              <AccordionSummary
                aria-controls="panel1-content"
                id="panel1-header"
              >
                <Typography className="text-color-green" fontWeight="600">
                  Admin
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                <QueryBuilder
                  fields={fields(allDataObj)}
                  operators={customOperators}
                  controlElements={{ valueEditor: CustomValueEditor }}
                  controlClassnames={{
                    queryBuilder: "queryBuilder-branches"
                  }}
                  query={queryBuilder}
                  onQueryChange={handleQueryChange}
                  combinators={[
                    { label: "AND", value: "and" },
                    { label: "OR", value: "or" }
                  ]}
                  translations={{
                    addGroup: {
                      label: "Add filter group"
                    },
                    addRule: {
                      label: "Add filter"
                    }
                  }}
                />
              </AccordionDetails>
            </Accordion>

            <Stack
              direction="row"
              justifyContent={"flex-end"}
              padding="0.875rem"
              gap="1rem"
            >
              <Stack>
                <CustomButton
                  variant="contained"
                  size="small"
                  onClick={() => {
                    handleFilterClick();
                    handleCloseFilterPopover();
                  }}
                >
                  Apply
                </CustomButton>
              </Stack>
              <Stack>
                <CustomButton
                  variant="outlined"
                  // classes="table-filter-btn"
                  size="small"
                  onClick={() => {
                    handleCloseFilterPopover();
                  }}
                >
                  Close
                </CustomButton>
              </Stack>
            </Stack>
          </Stack>
        </Popover>
      }
    </>
  );
};

export default AuditFilters;
