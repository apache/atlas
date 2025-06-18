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

import { useState, useRef } from "react";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import FormControl from "@mui/material/FormControl";
import TextField from "@mui/material/TextField";
import MenuItem from "@mui/material/MenuItem";
import IconButton from "@mui/material/IconButton";
import InputBase from "@mui/material/InputBase";
import Paper from "@mui/material/Paper";
import KeyboardArrowLeft from "@mui/icons-material/KeyboardArrowLeft";
import KeyboardArrowRight from "@mui/icons-material/KeyboardArrowRight";
import { toast } from "react-toastify";
import { isEmpty } from "@utils/Utils";
import Autocomplete, { createFilterOptions } from "@mui/material/Autocomplete";
import { pageSizeOptions } from "@utils/Enum";
import Messages from "@utils/Messages";

const filter = createFilterOptions<any>();

export const TableClientsidePagination = ({
  getPageCount,
  pagination,
  previousPage,
  showGoToPage,
  nextPage,
  setPageSize,
  getRowModel,
  setPageIndex,
  memoizedData
}: any) => {
  const { pageIndex, pageSize } = pagination;
  const pageCount = getPageCount();
  const toastId = useRef<any>(null);
  const [pendingGoToPageVal, setPendingGoToPageVal] = useState("");
  const gotoPageInputRef = useRef<HTMLInputElement>(null);
  const [value, setValue] = useState({ label: pageSize.toString() });

  const handleChange = (newValue: { label: string }) => {
    const val = parseInt(newValue.label, 10);
    if (!isNaN(val)) {
      setPageSize(val);
      setValue({ label: val.toString() });
      setPendingGoToPageVal("");
      setPageIndex(0);
    }
  };

  const handleGoClick = () => {
    const val = parseInt(pendingGoToPageVal, 10);
    if (isNaN(val) || val < 1) return;
    if (val > pageCount) {
      toast.dismiss(toastId.current);
      toastId.current = toast.info(
        <>
          {Messages.search.noRecordForPage} page "
          <strong>{pendingGoToPageVal}</strong>"
        </>
      );

      setPendingGoToPageVal("");

      return;
    }
    if (val - 1 === pageIndex) {
      toast.info("You are already on the same page");
      setPendingGoToPageVal("");

      return;
    }
    setPageIndex(val - 1);
    setPendingGoToPageVal("");
  };

  const offset = pageIndex * pageSize;
  const totalRows = getRowModel().rows.length;
  const from = totalRows === 0 ? 0 : offset + 1;
  const to = Math.min(offset + pageSize, totalRows);

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
          Showing <u>{totalRows.toLocaleString()} records</u> From {from} - {to}
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
            fontWeight="400"
            lineHeight="32px"
          >
            Page Limit :
          </Typography>
          <FormControl fullWidth size="small">
            <Autocomplete
              value={value}
              className="pagination-page-limit"
              disableClearable
              onChange={(_, newValue: any) => {
                if (typeof newValue === "string") {
                  handleChange({ label: newValue });
                } else if (newValue?.inputValue) {
                  handleChange({ label: newValue.inputValue });
                } else if (newValue) {
                  handleChange(newValue);
                }
              }}
              filterOptions={(options, params) => {
                const filtered = filter(options, params);
                const { inputValue } = params;
                const isExisting = options.some(
                  (option) => inputValue === option.label
                );
                if (inputValue !== "" && !isExisting) {
                  filtered.push({ inputValue, label: `${inputValue}` });
                }
                return filtered;
              }}
              selectOnFocus
              clearOnBlur
              handleHomeEndKeys
              id="Page Limit:"
              options={pageSizeOptions}
              size="small"
              getOptionLabel={(option) =>
                typeof option === "string"
                  ? option
                  : option.inputValue || option.label
              }
              renderOption={(props, option) => (
                <MenuItem {...props} value={option.label}>
                  {option.label}
                </MenuItem>
              )}
              sx={{
                width: "78px",
                "& .MuiOutlinedInput-root.MuiInputBase-sizeSmall .MuiAutocomplete-input":
                  {
                    padding: "0 4px !important",
                    height: "15px !important"
                  }
              }}
              freeSolo
              renderInput={(params) => <TextField type="number" {...params} />}
            />
          </FormControl>
        </Stack>

        {!(memoizedData.length < pageSize) && (
          <>
            {showGoToPage && (
              <Stack className="table-pagination-filters-box">
                <Paper
                  component="form"
                  elevation={0}
                  className="table-pagination-gotopage-paper"
                >
                  <InputBase
                    placeholder="Go to page:"
                    type="number"
                    inputProps={{
                      "aria-label": "Go to page:",
                      style: { width: "100%" }
                    }}
                    size="small"
                    onChange={(e) => setPendingGoToPageVal(e.target.value)}
                    ref={gotoPageInputRef}
                    className="table-pagination-gotopage-input"
                    value={pendingGoToPageVal}
                  />
                  <IconButton
                    type="button"
                    size="small"
                    className={`${
                      !isEmpty(pendingGoToPageVal)
                        ? "cursor-pointer"
                        : "cursor-not-allowed"
                    } table-pagination-gotopage-button`}
                    aria-label="search"
                    onClick={handleGoClick}
                    disabled={isEmpty(pendingGoToPageVal)}
                  >
                    Go
                  </IconButton>
                </Paper>
              </Stack>
            )}

            <Stack flexDirection="row" alignItems="center">
              <IconButton
                size="small"
                className="pagination-page-change-btn"
                onClick={() => previousPage()}
                disabled={pageIndex === 0}
                aria-label="previous page"
              >
                <KeyboardArrowLeft />
              </IconButton>

              <IconButton size="small" className="table-pagination-page">
                {pageIndex + 1}
              </IconButton>

              <IconButton
                size="small"
                className="pagination-page-change-btn"
                onClick={() => nextPage()}
                disabled={pageIndex + 1 >= pageCount}
                aria-label="next page"
              >
                <KeyboardArrowRight />
              </IconButton>
            </Stack>
          </>
        )}
      </div>
    </Stack>
  );
};

export default TableClientsidePagination;
