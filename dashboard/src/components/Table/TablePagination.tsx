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

import * as React from "react";
import {
  Autocomplete,
  FormControl,
  IconButton,
  InputBase,
  MenuItem,
  Pagination,
  Paper,
  Stack,
  TextField,
  Typography,
  createFilterOptions,
  styled
} from "@mui/material";

import { useTheme } from "@emotion/react";
import { KeyboardArrowLeft, KeyboardArrowRight } from "@mui/icons-material";
import { LightTooltip } from "../muiComponents";
import { useLocation, useNavigate } from "react-router-dom";
import { isEmpty } from "../../utils/Utils";

export const StyledPagination = styled(Pagination)`
  display: flex;
  justify-content: center;
  margin-top: 1rem;
`;

interface PaginationProps {
  firstPage?: any;
  getCanPreviousPage?: any;
  previousPage?: any;
  nextPage?: any;
  getCanNextPage?: any;
  lastPage?: any;
  getPageCount?: any;
  setPageIndex?: any;
  setPageSize?: any;
  getRowModel?: any;
  getRowCount?: any;
  pagination?: any;
  setRowSelection?: any;
  memoizedData?: any;
  isFirstPage?: any;
  isClientSidePagination?: any;
  setPagination?: any;
}
interface FilmOptionType {
  inputValue?: string;
  label: string;
}

const optionsVal = [
  { label: "25" },
  { label: "50" },
  { label: "100" },
  { label: "150" },
  { label: "200" },
  { label: "250" },
  { label: "300" },
  { label: "350" },
  { label: "400" },
  { label: "450" },
  { label: "500" }
];
const filter = createFilterOptions<FilmOptionType>();

const options: readonly FilmOptionType[] = optionsVal;
const TablePagination: React.FC<PaginationProps> = ({
  setPageIndex,
  setPageSize,
  getRowModel,
  pagination,
  setRowSelection,
  memoizedData,
  isFirstPage,
  setPagination
}) => {
  const theme: any = useTheme();
  const location = useLocation();
  const navigate = useNavigate();
  const searchParams: any = new URLSearchParams(location.search);
  const [inputVal, setInputVal] = React.useState<any>("");
  const [value, setValue] = React.useState<any>(
    searchParams.get("pageLimit") != null
      ? searchParams.get("pageLimit")
      : options[0].label
  );
  const { pageSize, pageIndex } = pagination;
  const handleChange = (newValue: any) => {
    setPageSize(newValue.label);
    setRowSelection({});
    searchParams.delete("pageOffset");
    searchParams.set("pageLimit", newValue.label);
    navigate({ search: searchParams.toString() });
  };

  const [_searchState, setSearchState] = React.useState(
    Object.fromEntries(searchParams)
  );

  React.useEffect(() => {
    setSearchState(Object.fromEntries(searchParams));
  }, [location.search]);

  let limit =
    parseInt(
      searchParams.get("pageLimit") !== undefined &&
        searchParams.get("pageLimit") !== null
        ? searchParams.get("pageLimit")
        : pageSize || "0",
      10
    ) || 0;
  let pageFrom = isFirstPage ? 1 : pageSize * pageIndex + 1;
  let pageTo = isFirstPage ? Number(limit) : pageSize * (Number(pageIndex) + 1);

  return (
    <Stack
      spacing={{ xs: 1, sm: 2 }}
      direction="row"
      useFlexGap
      flexWrap="wrap"
      justifyContent="space-between"
      alignItems="center"
      className="table-pagination"
    >
      <div>
        <span className="text-grey">
          Showing <u>{getRowModel()?.rows?.length.toLocaleString()} records</u>{" "}
          From {pageFrom} - {pageTo}
        </span>
      </div>
      <div className="table-pagination-filters">
        <Stack
          className="table-pagination-filters-box"
          direction="row"
          alignItems="center"
          gap="0.5rem"
        >
          <Typography
            className="text-grey"
            whiteSpace="nowrap"
            fontWeight="600"
            lineHeight="32px"
          >
            Page Limit :
          </Typography>
          <FormControl fullWidth size="small">
            <Autocomplete
              value={value}
              className="pagination-page-limit"
              disableClearable
              onChange={(_event: any, newValue) => {
                if (typeof newValue === "string") {
                  setValue({
                    label: newValue
                  });
                  handleChange({ label: newValue });
                } else if (newValue && newValue.inputValue) {
                  setValue({
                    label: newValue.inputValue
                  });
                  handleChange({ label: newValue.inputValue });
                } else if (newValue) {
                  setValue(newValue);
                  handleChange(newValue);
                } else {
                  const fallbackValue = { label: inputVal || "" };
                  setValue(fallbackValue);
                  handleChange(fallbackValue);
                }
                setPagination((prev: { pageIndex: number }) => ({
                  ...prev,
                  pageIndex: 0
                }));
              }}
              filterOptions={(options, params) => {
                const filtered = filter(options, params);

                const { inputValue } = params;

                const isExisting = options.some(
                  (option) => inputValue === option.label
                );
                if (inputValue !== "" && !isExisting) {
                  filtered.push({
                    inputValue,
                    label: `${inputValue}`
                  });
                }

                return filtered;
              }}
              defaultValue={
                searchParams.get("pageLimit") != null &&
                searchParams.get("pageLimit")
              }
              selectOnFocus
              clearOnBlur
              handleHomeEndKeys
              id="Page Limit:"
              options={options}
              size="small"
              getOptionLabel={(option) => {
                if (typeof option === "string") {
                  return option;
                }
                if (option.inputValue) {
                  return option.inputValue;
                }
                return option.label;
              }}
              renderOption={(props, option) => (
                <MenuItem {...props} value={option.label}>
                  {option.label}
                </MenuItem>
              )}
              sx={{
                width: "78px"
              }}
              freeSolo
              renderInput={(params) => <TextField type="number" {...params} />}
            />
          </FormControl>
        </Stack>
        {isFirstPage &&
        (!memoizedData.length || memoizedData.length < pagination.pageSize) ? (
          ""
        ) : (
          <>
            <Stack className="table-pagination-filters-box">
              <Paper
                component="form"
                elevation={0}
                className="table-pagination-gotopage-paper"
              >
                <InputBase
                  placeholder="Go to page:"
                  type="number"
                  inputProps={{ "aria-label": "Go to page:" }}
                  size="small"
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                    const page = e.target.value
                      ? Number(e.target.value) - 1
                      : 0;
                    setInputVal(page);
                  }}
                  className="table-pagination-gotopage-input"
                  defaultValue={inputVal}
                />
                <LightTooltip title="Goto Page">
                  <IconButton
                    type="button"
                    size="small"
                    className={`${
                      !isEmpty(inputVal)
                        ? "cursor-pointer"
                        : "cursor-not-allowed"
                    } table-pagination-gotopage-button`}
                    aria-label="search"
                    onClick={() => {
                      if (!isEmpty(inputVal)) {
                        if (inputVal >= 1) {
                          setPageIndex(inputVal);
                          searchParams.set(
                            "pageOffset",
                            pagination.pageSize * inputVal
                          );
                          navigate({ search: searchParams.toString() });
                        } else {
                          setPageIndex(0);
                          searchParams.delete("pageOffset");
                          navigate({ search: searchParams.toString() });
                        }
                        setInputVal("");
                      } else {
                        return;
                      }
                      setRowSelection({});
                    }}
                  >
                    Go
                  </IconButton>
                </LightTooltip>
              </Paper>
            </Stack>

            <Stack flexDirection="row" alignItems="center">
              <LightTooltip title="Previous">
                <IconButton
                  size="small"
                  className="pagination-previous-btn"
                  onClick={() => {
                    setPagination((prev: { pageIndex: number }) => ({
                      ...prev,
                      pageIndex: prev.pageIndex - 1
                    }));
                    setRowSelection({});
                    if (isFirstPage) {
                      searchParams.delete("pageOffset");
                    } else {
                      searchParams.set(
                        "pageOffset",
                        `${pagination.pageSize * (pagination.pageIndex - 1)}`
                      );
                      navigate({ search: searchParams.toString() });
                    }
                  }}
                  disabled={isFirstPage}
                  aria-label="previous page"
                >
                  {theme.direction === "rtl" ? (
                    <KeyboardArrowRight />
                  ) : (
                    <KeyboardArrowLeft />
                  )}
                </IconButton>
              </LightTooltip>
              <LightTooltip title={`Page ${Math.round(pageTo / limit)}`}>
                <IconButton size="small" className="table-pagination-page">
                  {Math.round(pageIndex + 1)}
                </IconButton>
              </LightTooltip>

              <LightTooltip title="Next">
                <IconButton
                  size="small"
                  className="pagination-next-btn"
                  onClick={() => {
                    setPagination((prev: { pageIndex: number }) => ({
                      ...prev,
                      pageIndex: prev.pageIndex + 1
                    }));
                    setRowSelection({});

                    searchParams.set(
                      "pageOffset",
                      `${pagination.pageSize * (pagination.pageIndex + 1)}`
                    );
                    navigate({ search: searchParams.toString() });
                  }}
                  disabled={memoizedData.length < limit}
                  aria-label="next page"
                >
                  {theme.direction === "rtl" ? (
                    <KeyboardArrowLeft />
                  ) : (
                    <KeyboardArrowRight />
                  )}
                </IconButton>
              </LightTooltip>
            </Stack>
          </>
        )}
      </div>
    </Stack>
  );
};

export default TablePagination;
