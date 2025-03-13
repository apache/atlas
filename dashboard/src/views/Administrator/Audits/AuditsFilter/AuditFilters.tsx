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
import { GlobalQueryState } from "@utils/Global";
import { isEmpty } from "@utils/Utils";

import { attributeFilter } from "@utils/CommonViewFunction";
import { cloneDeep } from "@utils/Helper";
import { timeRangeOptions } from "@utils/Enum";
import { DemoContainer } from "@mui/x-date-pickers/internals/demo";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterMoment } from "@mui/x-date-pickers/AdapterMoment";
import { DateTimePicker } from "@mui/x-date-pickers/DateTimePicker";

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
    if (selectedValue === "custom_range") {
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
          <DatePicker
            selectsRange
            showTimeSelect
            showPopperArrow={false}
            popperProps={{ strategy: "fixed" }}
            showYearDropdown
            showMonthDropdown
            startDate={
              moment(startDate).isValid()
                ? moment(startDate).toDate()
                : undefined
            }
            endDate={
              moment(endDate).isValid() ? moment(endDate).toDate() : undefined
            }
            onChange={(update: [Date | null, Date | null] | null) => {
              setDateRange(update);
              props.handleOnChange(update).join(",");
            }}
            isClearable={true}
          />
        )}
      </div>
    );
  }

  if (props.inputType == "datetime-local") {
    return (
      <LocalizationProvider dateAdapter={AdapterMoment}>
        <DemoContainer
          components={["DatePicker"]}
          sx={{
            overflow: "hidden",
            marginBottom: "0.5rem"
          }}
        >
          <DateTimePicker
            views={["year", "day", "hours", "minutes", "seconds"]}
            slotProps={{
              textField: {
                size: "small",
                sx: {
                  "& .MuiInputBase-root": {
                    height: "34px",
                    alignItems: "center"
                  },
                  "& .MuiInputBase-input": {
                    padding: "10px 14px"
                  }
                }
              }
            }}
            onChange={(value: moment.Moment | null) => {
              // setSelectedDateValue(value ? value : moment());
              // props.handleOnChange(value ? value.toISOString() : null);
              const isoString = value
                ? value.format("MM/DD/YYYY hh:mm:ss A")
                : null;
              setSelectedDateValue(value);
              props.handleOnChange(isoString);
            }}
            timeSteps={{ hours: 1, minutes: 1, seconds: 1 }}
            // value={props.value ? moment(props.value) : moment()}
            value={selectedDateValue}
            defaultValue={moment()}
            // onChange={(date) => {
            //   props.handleOnChange(date ? date.toISOString() : null);
            // }}
            // onChange={handleDateChange}
          />
        </DemoContainer>
      </LocalizationProvider>
    );
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

  // useEffect(() => {
  //   if (!isEmpty(GlobalQueryState.getQuery())) {
  //     setupdateTable(moment.now());
  //   }
  // }, []);

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
