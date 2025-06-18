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

import { useState, useRef, useEffect } from "react";
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
  Typography
} from "@mui/material";
import { useTheme } from "@emotion/react";
import { KeyboardArrowLeft, KeyboardArrowRight } from "@mui/icons-material";
import { LightTooltip } from "../muiComponents";
import { useLocation, useNavigate } from "react-router-dom";
import { isEmpty } from "../../utils/Utils";
import Messages from "@utils/Messages";
import { toast } from "react-toastify";
import { GetNumberSuffix } from "@components/commonComponents";
import { pageSizeOptions } from "@utils/Enum";
import { createFilterOptions } from "@mui/material/Autocomplete";
import { styled } from "@mui/material/styles";

const filter = createFilterOptions<any>();

export const StyledPagination = styled(Pagination)`
  display: flex;
  justify-content: center;
  margin-top: 1rem;
`;

interface PaginationProps {
  isServerSide?: boolean;
  getPageCount?: () => number;
  previousPage?: () => void;
  nextPage?: () => void;
  setPageIndex?: (index: number) => void;
  setPageSize?: (size: number) => void;
  getRowModel?: () => any;
  pagination: { pageIndex: number; pageSize: number };
  setRowSelection?: (selection: any) => void;
  memoizedData: any[];
  isFirstPage?: boolean;
  setPagination?: (pagination: any) => void;
  goToPageVal?: string;
  setGoToPageVal?: React.Dispatch<React.SetStateAction<string>>;
  isEmptyData?: boolean;
  setIsEmptyData?: any;
  showGoToPage?: boolean;
}

const TablePagination: React.FC<PaginationProps> = ({
  isServerSide = false,
  getPageCount,
  previousPage,
  nextPage,
  setPageIndex,
  setPageSize,
  getRowModel,
  pagination,
  setRowSelection,
  memoizedData,
  isFirstPage,
  setPagination,
  goToPageVal,
  setGoToPageVal,
  isEmptyData,
  setIsEmptyData,
  showGoToPage = false
}) => {
  const theme: any = useTheme();
  const location = useLocation();
  const navigate = useNavigate();
  const searchParams = new URLSearchParams(location.search);
  const { pageIndex, pageSize } = pagination;

  const [value, setValue] = useState<any>({
    label:
      searchParams.get("pageLimit") ??
      pageSize.toString() ??
      pageSizeOptions[0].label
  });
  const [limit, setLimit] = useState<number>(
    searchParams.get("pageLimit")
      ? Number(searchParams.get("pageLimit"))
      : pageSize
  );

  const [pageFrom, setPageFrom] = useState<number>(
    isServerSide && searchParams.get("pageOffset")
      ? Number(searchParams.get("pageOffset")) + 1
      : isServerSide
      ? 1
      : pageIndex * pageSize + 1
  );
  const [pageTo, setPageTo] = useState<number>(
    isServerSide && searchParams.get("pageOffset")
      ? Number(searchParams.get("pageOffset")) + Number(limit)
      : isServerSide
      ? Number(limit)
      : Math.min((pageIndex + 1) * pageSize, getRowModel?.().rows.length || 0)
  );
  const [offset, setOffset] = useState<number>(
    isServerSide && searchParams.get("pageOffset")
      ? Number(searchParams.get("pageOffset"))
      : pageIndex * pageSize
  );
  const [activePage, setActivePage] = useState<number>(
    isServerSide ? Math.round(pageTo / Number(limit)) : pageIndex + 1
  );

  const [pendingGoToPageVal, setPendingGoToPageVal] = useState<string>("");
  const [goToPageTrigger, setGoToPageTrigger] = useState<string>("");
  const toastId = useRef<any>(null);
  const finalPage = useRef<any>(activePage);
  const gotoPageCurrentVal = useRef<string>("");

  useEffect(() => {
    if (isServerSide) {
      setValue({ label: searchParams.get("pageLimit") ?? limit.toString() });
      setLimit(Number(searchParams.get("pageLimit") ?? limit));
      setOffset(Number(searchParams.get("pageOffset") ?? pageIndex * pageSize));
      setPageFrom(
        Number(searchParams.get("pageOffset") ?? pageIndex * pageSize) + 1
      );
      setPageTo(
        Number(searchParams.get("pageOffset") ?? pageIndex * pageSize) +
          Number(limit)
      );
    }
  }, [isServerSide, location.search, limit, pageIndex, pageSize]);

  useEffect(() => {
    if (isServerSide && isFirstPage) {
      setPageFrom(1);
      setPageTo(limit);
      setOffset(0);
    }
  }, [isServerSide, isFirstPage, limit]);

  useEffect(() => {
    setActivePage(
      isServerSide ? Math.round(pageTo / Number(limit)) : pageIndex + 1
    );
  }, [isServerSide, pageTo, limit, pageIndex]);

  const handlePageSizeChange = (newValue: any) => {
    const newPageSize: any = Number(newValue.label);
    setValue({ label: newValue.label });
    if (isServerSide) {
      setLimit(newPageSize);
      setPageFrom(1);
      setPageTo(newPageSize);
      setActivePage(1);
      setOffset(0);
      searchParams.delete("pageOffset");
      searchParams.set("pageLimit", newPageSize);
      navigate({ search: searchParams.toString() });
      setRowSelection?.({});
      setPagination?.((prev: any) => ({ ...prev, pageIndex: 0 }));
    } else {
      setPageSize?.(newPageSize);
      setPageIndex?.(0);
      setLimit(newPageSize);
      setPageFrom(1);
      setPageTo(Math.min(newPageSize, getRowModel?.().rows.length || 0));
    }
  };

  const handleGoToPage = () => {
    const goToPage = parseInt(goToPageVal || pendingGoToPageVal);
    if (isNaN(goToPage) || goToPage < 1) return;

    if (isServerSide) {
      if (goToPage > (finalPage.current || Infinity)) {
        toast.dismiss(toastId.current);
        toastId.current = toast.info(
          <>
            {Messages.search.noRecordForPage}
            <b>
              <GetNumberSuffix number={goToPage} sup={true} />
            </b>{" "}
            page
          </>
        );
        setGoToPageVal?.("");
        setPendingGoToPageVal("");
        return;
      }
      const newOffset = (goToPage - 1) * limit;
      if (newOffset === offset) {
        toast.dismiss(toastId.current);
        toastId.current = toast.info(`${Messages.search.onSamePage}`);
        setGoToPageVal?.("");
        setPendingGoToPageVal("");
        return;
      }
      setOffset(newOffset);
      setPageFrom(newOffset + 1);
      setPageTo(newOffset + limit);
      setActivePage(goToPage);
      setPageIndex?.(goToPage - 1);
      setRowSelection?.({});
      searchParams.set("pageOffset", newOffset.toString());
      navigate({ search: searchParams.toString() });
      setGoToPageVal?.("");
      setPendingGoToPageVal("");
    } else {
      if (goToPage > (getPageCount?.() || Infinity)) {
        toast.dismiss(toastId.current);
        toastId.current = toast.info(
          <>
            {Messages.search.noRecordForPage} page "<strong>{goToPage}</strong>"
          </>
        );
        setPendingGoToPageVal("");
        return;
      }
      if (goToPage - 1 === pageIndex) {
        toast.dismiss(toastId.current);
        toastId.current = toast.info(`${Messages.search.onSamePage}`);
        setPendingGoToPageVal("");
        return;
      }
      setPageIndex?.(goToPage - 1);
      setPageFrom((goToPage - 1) * pageSize + 1);
      setPageTo(
        Math.min(goToPage * pageSize, getRowModel?.().rows.length || 0)
      );
      setPendingGoToPageVal("");
    }
  };

  useEffect(() => {
    if ((goToPageVal || goToPageTrigger || isEmptyData) && setPageIndex) {
      handleGoToPage();
      if (typeof setIsEmptyData === "function") {
        setIsEmptyData(false);
      }
      setGoToPageTrigger("");
    }
  }, [goToPageVal, goToPageTrigger, isEmptyData]);

  const handlePreviousPage = () => {
    if (isServerSide) {
      const prevOffset = offset - limit;
      const safePrevOffset = prevOffset < 0 ? 0 : prevOffset;
      setOffset(safePrevOffset);
      setPageFrom(safePrevOffset + 1);
      setPageTo(safePrevOffset + limit);
      setPagination?.((prev: any) => ({
        ...prev,
        pageIndex: prev.pageIndex - 1
      }));
      setRowSelection?.({});
      if (isFirstPage) {
        searchParams.delete("pageOffset");
      } else {
        searchParams.set("pageOffset", safePrevOffset.toString());
        navigate({ search: searchParams.toString() });
      }
    } else {
      previousPage?.();
      setPageFrom(pageIndex * pageSize + 1);
      setPageTo(
        Math.min(pageIndex * pageSize, getRowModel?.().rows.length || 0)
      );
    }
    setGoToPageVal?.("");
  };

  const handleNextPage = () => {
    if (isServerSide) {
      const nextOffset = offset + limit;
      setOffset(nextOffset);
      setPageFrom(nextOffset + 1);
      setPageTo(nextOffset + limit);
      setPagination?.((prev: any) => ({
        ...prev,
        pageIndex: prev.pageIndex + 1
      }));
      setRowSelection?.({});
      searchParams.set("pageOffset", nextOffset.toString());
      navigate({ search: searchParams.toString() });
    } else {
      nextPage?.();
      setPageFrom((pageIndex + 1) * pageSize + 1);
      setPageTo(
        Math.min((pageIndex + 2) * pageSize, getRowModel?.().rows.length || 0)
      );
    }
    setGoToPageVal?.("");
  };

  const isPreviousDisabled = isServerSide ? isFirstPage : pageIndex === 0;
  const isNextDisabled = isServerSide
    ? memoizedData.length < limit
    : pageIndex + 1 >= (getPageCount?.() || Infinity);

  const totalRows = getRowModel?.().rows.length || 0;
  const displayFrom = isServerSide
    ? pageFrom
    : totalRows === 0
    ? 0
    : pageIndex * pageSize + 1;
  const displayTo = isServerSide
    ? pageTo
    : Math.min((pageIndex + 1) * pageSize, totalRows);

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
          Showing <u>{totalRows.toLocaleString()} records</u> From {displayFrom}{" "}
          - {displayTo}
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
                  handlePageSizeChange({ label: newValue });
                } else if (newValue?.inputValue) {
                  handlePageSizeChange({ label: newValue.inputValue });
                } else if (newValue) {
                  handlePageSizeChange(newValue);
                }
              }}
              filterOptions={(options, params) => {
                const filtered = filter(options, params);
                const { inputValue } = params;
                const isExisting = options.some(
                  (option) => inputValue === option.label
                );
                if (inputValue !== "" && setPageSize && !isExisting) {
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

        {(isServerSide
          ? !isFirstPage || memoizedData.length >= limit
          : memoizedData.length >= pageSize) && (
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
                    onChange={(e) => {
                      setPendingGoToPageVal(e.target.value);
                      gotoPageCurrentVal.current = e.target.value;
                    }}
                    onKeyUp={(e) => {
                      const goToPage = parseInt(e.currentTarget.value);
                      if (e.which === 13 && !isNaN(goToPage) && goToPage >= 1) {
                        setGoToPageVal?.(e.currentTarget.value);
                        setGoToPageTrigger(e.currentTarget.value);
                      }
                    }}
                    className="table-pagination-gotopage-input"
                    value={pendingGoToPageVal}
                  />
                  <LightTooltip title="Goto Page">
                    <IconButton
                      type="button"
                      size="small"
                      className={`${
                        !isEmpty(pendingGoToPageVal)
                          ? "cursor-pointer"
                          : "cursor-not-allowed"
                      } table-pagination-gotopage-button`}
                      aria-label="search"
                      onClick={() => {
                        if (!isEmpty(pendingGoToPageVal)) {
                          setGoToPageTrigger(pendingGoToPageVal);
                          handleGoToPage();
                        }
                      }}
                      disabled={isEmpty(pendingGoToPageVal)}
                    >
                      Go
                    </IconButton>
                  </LightTooltip>
                </Paper>
              </Stack>
            )}

            <Stack flexDirection="row" alignItems="center">
              <LightTooltip title="Previous">
                <IconButton
                  size="small"
                  className="pagination-page-change-btn"
                  onClick={handlePreviousPage}
                  disabled={isPreviousDisabled}
                  aria-label="previous page"
                >
                  {theme.direction === "rtl" ? (
                    <KeyboardArrowRight />
                  ) : (
                    <KeyboardArrowLeft />
                  )}
                </IconButton>
              </LightTooltip>

              <LightTooltip title={`Page ${activePage}`}>
                <IconButton size="small" className="table-pagination-page">
                  {activePage}
                </IconButton>
              </LightTooltip>

              <LightTooltip title="Next">
                <IconButton
                  size="small"
                  className="pagination-page-change-btn"
                  onClick={handleNextPage}
                  disabled={isNextDisabled}
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
