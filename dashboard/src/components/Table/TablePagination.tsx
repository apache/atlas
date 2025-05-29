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
import Messages from "@utils/Messages";
import { toast } from "react-toastify";
import { GetNumberSuffix } from "@components/commonComponents";
import { useEffect, useRef, useState } from "react";

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
  goToPageVal?: string | undefined;
  setGoToPageVal?: React.Dispatch<React.SetStateAction<string>>;
  isEmptyData?: boolean;
  setIsEmptyData?: React.Dispatch<React.SetStateAction<boolean>>;
  showGoToPage?: boolean;
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
  setPagination,

  isEmptyData,
  goToPageVal,
  setGoToPageVal,
  setIsEmptyData,
  showGoToPage = false
}) => {
  const theme: any = useTheme();
  const location = useLocation();
  const navigate = useNavigate();
  const searchParams: any = new URLSearchParams(location.search);
  const [pendingGoToPageVal, setPendingGoToPageVal] = useState<string>("");
  const [goToPageTrigger, setGoToPageTrigger] = useState<string>("");
  const [value, setValue] = useState<any>(
    searchParams.get("pageLimit") != null
      ? searchParams.get("pageLimit")
      : options[0].label
  );
  const [limit, setPageLimit] = useState<number>(
    searchParams.get("pageLimit") != null
      ? Number(searchParams.get("pageLimit"))
      : 25
  );

  const [pageFrom, setPageFrom] = useState(
    searchParams.get("pageOffset") != null
      ? Number(searchParams.get("pageOffset")) + 1
      : 1
  );
  const [pageTo, setPageTo] = useState(
    searchParams.get("pageOffset") != null
      ? Number(searchParams.get("pageOffset")) + Number(limit)
      : Number(limit)
  );
  const [activePage, setActivePage] = useState(
    Math.round(pageTo / Number(limit))
  );
  const { pageSize, pageIndex } = pagination;

  const [offset, setOffset] = useState<number>(
    searchParams.get("pageOffset") != null
      ? Number(searchParams.get("pageOffset"))
      : pageIndex * pageSize || 0
  );
  const gotopageRef = useRef<any>(null);

  const handleChange = (newValue: any) => {
    setPageSize(Number(newValue.label));
    setRowSelection({});
    setPageFrom(1);
    setPageTo(Number(newValue.label));
    setActivePage(1);
    setOffset(0);
    setPageLimit(Number(newValue.label));
    searchParams.delete("pageOffset");
    searchParams.set("pageLimit", Number(newValue.label));
    navigate({ search: searchParams.toString() });
  };

  const [_searchState, setSearchState] = useState(
    Object.fromEntries(searchParams)
  );
  const toastId = useRef<any>(null);
  const gotoPagebtnDisabled = useRef(false);
  const gotoPageCurrentVal = useRef("");

  const finalPage = useRef();

  gotopageRef.current = goToPageVal;

  useEffect(() => {
    setSearchState(Object.fromEntries(searchParams));
  }, [location.search]);

  useEffect(() => {
    if (isFirstPage) {
      setPageFrom(1);
      setPageTo(limit);
    }
  }, [isFirstPage, limit]);

  useEffect(() => {
    setActivePage(Math.round(pageTo / limit));
  }, [pageTo, limit]);

  const gotoPagebtn = () => {
    const goToPage: number | undefined =
      goToPageVal !== ""
        ? parseInt(goToPageVal ?? "")
        : pendingGoToPageVal !== ""
        ? parseInt(pendingGoToPageVal)
        : 0;
    if (!(isNaN(goToPage) || goToPage <= -1) || pendingGoToPageVal) {
      if (finalPage.current && finalPage.current < goToPage) {
        toast.dismiss(toastId.current);
        toastId.current = toast.info(
          `${Messages.search.noRecordForPage} page  ${gotoPageCurrentVal.current}`
        );
        if (setGoToPageVal) setGoToPageVal("");
        setPendingGoToPageVal("");
        return;
      }

      const newOffset = (goToPage - 1) * limit;
      const safeOffset = newOffset <= -1 ? 0 : newOffset;
      if (safeOffset === pageFrom - 1) {
        toast.dismiss(toastId.current);
        toastId.current = toast.info(`${Messages.search.onSamePage}`);
      } else {
        gotopageRef.current = "";
        setPageIndex(goToPageVal);
        setOffset(safeOffset);
        setPageTo(safeOffset + limit);
        setPageFrom(safeOffset + 1);
        if (isEmptyData) {
          const emptyOffset = (activePage - 1) * limit;
          setOffset(emptyOffset);
          searchParams.set("pageOffset", emptyOffset);
          if (typeof setIsEmptyData === "function") {
            setIsEmptyData(false);
          }
        } else {
          searchParams.set("pageOffset", `${pageSize * (goToPage - 1)}`);
        }
        navigate({ search: searchParams.toString() });
      }
    }
  };

  useEffect(() => {
    if (goToPageVal !== "" || isEmptyData) {
      gotoPagebtn();
    }
  }, [gotopageRef.current, goToPageTrigger]);

  useEffect(() => {
    if (goToPageTrigger !== "" || isEmptyData) {
      const goToPage = parseInt(goToPageTrigger);
      if (!(isNaN(goToPage) || goToPage <= -1) || isEmptyData) {
        const newOffset2 = (goToPage - 1) * limit;
        const safeOffset2 = newOffset2 <= -1 ? 0 : newOffset2;
        if (!isEmptyData) {
          setOffset(safeOffset2);
          setPageFrom(safeOffset2 + 1);
          setPageTo(safeOffset2 + limit);
          setActivePage(Math.round(pageTo / limit));
        }
        if (isEmptyData) {
          searchParams.delete("pageOffset");
          const goToPageNum =
            gotoPageCurrentVal.current !== ""
              ? parseInt(gotoPageCurrentVal.current ?? "")
              : 0;
          setOffset(pageSize * pageIndex);
          toast.dismiss(toastId.current);
          if (!toast.isActive(toastId.current)) {
            toastId.current = toast.info(
              <>
                {Messages.search.noRecordForPage}
                <b>
                  <GetNumberSuffix
                    number={Math.round(goToPageNum)}
                    sup={true}
                  />
                </b>{" "}
                page
              </>
            );
          }

          if (typeof setIsEmptyData === "function") {
            setIsEmptyData(false);
          }
        } else {
          searchParams.set("pageOffset", `${pageSize * (goToPage - 1)}`);
        }
        navigate({ search: searchParams.toString() });
        gotopageRef.current = "";
        setGoToPageTrigger("");
        setPendingGoToPageVal("");
      }
    }
  }, [isEmptyData, goToPageTrigger]);

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
              onChange={(_event: any, newValue) => {
                // limit = parseInt(newValue);
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
                  const fallbackValue = { label: goToPageVal || "" };
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
        {isFirstPage &&
        (!memoizedData.length || memoizedData.length < pageSize) ? (
          ""
        ) : (
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
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                      setPendingGoToPageVal(e.target.value);
                      gotoPagebtnDisabled.current = !e.target.value;
                      gotoPageCurrentVal.current = e.target.value;
                    }}
                    ref={gotoPageCurrentVal}
                    onKeyUp={(e) => {
                      let code = e.which;
                      let goToPage = parseInt(e.currentTarget.value);
                      if (goToPage) {
                        gotoPagebtnDisabled.current = false;
                        setPendingGoToPageVal(e.currentTarget.value);
                      } else {
                        gotoPagebtnDisabled.current = true;
                        setPendingGoToPageVal("");
                      }
                      if (code == 13 && e.currentTarget.value) {
                        if (setGoToPageVal) setGoToPageVal("");
                        if (setGoToPageVal)
                          setTimeout(
                            () => setGoToPageVal(pendingGoToPageVal),
                            0
                          );
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
                          gotoPagebtn();
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
                  onClick={() => {
                    const prevOffset = Number(offset) - Number(limit);
                    const safePrevOffset = prevOffset <= -1 ? 0 : prevOffset;
                    let pageToVal = pageTo - limit;
                    setOffset(safePrevOffset);
                    setPageTo(pageToVal);
                    setPageFrom(pageToVal - limit + 1);
                    setPagination((prev: { pageIndex: number }) => ({
                      ...prev,
                      pageIndex: prevOffset
                    }));
                    setRowSelection({});
                    if (setGoToPageVal) setGoToPageVal("");
                    if (isFirstPage) {
                      searchParams.delete("pageOffset");
                    } else {
                      searchParams.set("pageOffset", prevOffset);
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
              <LightTooltip title={`Page ${activePage}`}>
                <IconButton size="small" className="table-pagination-page">
                  {Math.round(activePage)}
                </IconButton>
              </LightTooltip>

              <LightTooltip title="Next">
                <IconButton
                  size="small"
                  className="pagination-page-change-btn"
                  onClick={() => {
                    const nextOffset = Number(offset) + Number(limit);
                    setOffset(nextOffset);
                    setPageTo(nextOffset + Number(limit));
                    setPageFrom(nextOffset + 1);
                    setPagination((prev: { pageIndex: number }) => ({
                      ...prev,
                      pageIndex: prev.pageIndex + 1
                    }));
                    setRowSelection({});
                    if (setGoToPageVal) setGoToPageVal("");
                    searchParams.set("pageOffset", nextOffset);
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
