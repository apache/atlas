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
  CircularProgress,
  InputAdornment,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import { useRef, useState } from "react";
import { getGlobalSearchResult } from "../../api/apiMethods/searchApiMethod";
import DisplayImage from "../EntityDisplayImage";
import SearchIcon from "@mui/icons-material/Search";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { entityStateReadOnly } from "../../utils/Enum";
import {
  extractKeyValueFromEntity,
  isEmpty,
  serverError
} from "../../utils/Utils";
import parse from "autosuggest-highlight/parse";
import match from "autosuggest-highlight/match";
import ClickAwayListener from "@mui/material/ClickAwayListener";
import AdvancedSearch from "./AdvancedSearch";
import {
  HandleValuesType,
  QuickSearchOptionListType,
  SuggestionDataType
} from "../../models/globalSearchType";
import { AxiosResponse } from "axios";
import { CustomButton } from "@components/muiComponents";

const QuickSearch = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const toastId = useRef(null);
  const searchParams = new URLSearchParams(location.search);
  const [options, setOptions] = useState<any>([]);
  const [open, setOpen] = useState<boolean>(false);
  const [openAdvanceSearch, setOpenAdvanceSearch] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [value, setValue] = useState<string>("");

  const getData = async (searchTerm: string) => {
    let entities: QuickSearchOptionListType = [];
    let suggestionNames: QuickSearchOptionListType = [];
    let quickSearchResp: AxiosResponse | null = null;
    let suggestionSearchResp: AxiosResponse | null = null;
    try {
      setLoading(true);
      quickSearchResp = await getGlobalSearchResult("quick", {
        params: { query: searchTerm, limit: 5, offset: 0 }
      });
      setLoading(false);
    } catch (error) {
      setLoading(false);
      console.error("Error fetching quick search results:", error);
      serverError(error, toastId);
    }

    try {
      setLoading(true);
      suggestionSearchResp = await getGlobalSearchResult("suggestions", {
        params: { prefixString: searchTerm }
      });
      setLoading(false);
    } catch (error) {
      setLoading(false);
      console.error("Error fetching suggestion search results:", error);
      serverError(error, toastId);
    }
    const { searchResults = [] } = quickSearchResp?.data || {};
    const { suggestions = [] }: SuggestionDataType =
      suggestionSearchResp?.data || {};

    entities = !isEmpty(searchResults?.entities)
      ? searchResults?.entities?.map((entityDef: any) => {
          const { name }: { name: string; found: boolean; key: any } =
            extractKeyValueFromEntity(entityDef);
          return {
            title: `${name}`,
            parent: entityDef.typeName,
            types: "Entities",
            entityObj: entityDef
          };
        })
      : [{ title: "No Entities Found", types: "Entities" }];

    suggestionNames = !isEmpty(suggestions)
      ? suggestions.map((suggestion: any) => {
          return {
            title: `${suggestion}`,
            types: "Suggestions"
          };
        })
      : [{ title: "No Suggestions Found", types: "Suggestions" }];

    setOptions([...entities, ...suggestionNames]);
  };

  const onInputChange = (_event: any, value: string) => {
    // Sanitize input: remove any potential script tags and validate
    const sanitizedValue = value ? value.trim() : "";
    
    if (sanitizedValue) {
      setOpen(true);
      getData(sanitizedValue);
    } else {
      setOptions([]);
      setValue("");
      setOpen(false);
    }
  };

  const handleValues = (option: HandleValuesType | string | null) => {
    // Handle case when option is a string (direct search query)
    if (typeof option === "string") {
      const queryValue = option.trim();
      if (queryValue) {
        setOpen(false);
        setOptions([]);
        // URLSearchParams automatically encodes the value, preventing XSS
        // Additional validation: ensure query is not empty after trim
        const sanitizedQuery = queryValue || "*";
        searchParams.set("query", sanitizedQuery);
        searchParams.set("searchType", "basic");
        navigate(
          {
            pathname: `/search/searchResult`,
            search: searchParams.toString()
          },
          { replace: true }
        );
      }
      return;
    }

    // Handle case when option is null or undefined
    if (!option || typeof option !== "object") {
      return;
    }

    const { entityObj, title, types } = option;
    
    // Validate that title exists and is not undefined
    if (!title || title === "undefined" || typeof title !== "string") {
      return;
    }

    // Sanitize title: trim and validate
    const sanitizedTitle = title.trim();
    if (!sanitizedTitle) {
      return;
    }

    setOpen(false);
    setOptions([]);
    // URLSearchParams automatically encodes the value, preventing XSS
    searchParams.set("query", sanitizedTitle);
    searchParams.set("searchType", "basic");

    if (types === "Entities" && entityObj && entityObj.guid) {
      navigate(
        {
          pathname: `/detailPage/${entityObj.guid}`
        },
        { replace: true }
      );
    } else {
      navigate(
        {
          pathname: `/search/searchResult`,
          search: searchParams.toString()
        },
        { replace: true }
      );
    }
  };

  const handleClickAway = () => {
    setOpen(false);
  };

  const handleCloseModal = () => {
    setOpenAdvanceSearch(false);
  };

  return (
    <>
      <Stack
        direction="row"
        className="global-search-stack"
        alignItems="center"
        gap="0.5rem"
      >
        <ClickAwayListener onClickAway={handleClickAway}>
          <Autocomplete
            open={open}
            loading={loading}
            onKeyDown={(e) => {
              const code = e.keyCode || e.which;

              switch (code) {
                case 13: // Enter key
                  e.preventDefault();
                  const inputValue = (e.target as HTMLInputElement).value.trim();
                  
                  // If input is empty, use "*" for wildcard search (matching classic UI behavior)
                  const searchQuery = inputValue === "" ? "*" : inputValue;
                  
                  // Try to find exact match in options
                  const activeOption = options.find(
                    (option: { title: any }) =>
                      typeof option !== "string" &&
                      option.title === searchQuery
                  );
                  
                  if (activeOption) {
                    // If exact match found, use that option
                    handleValues(activeOption as HandleValuesType);
                  } else {
                    // If no match found, trigger basic search with typed value (matching classic UI behavior)
                    handleValues(searchQuery);
                  }
                  break;
                case 9: // Tab key
                case 27: // Escape key
                  setOpen(false);
                  break;
                default:
                  break;
              }
            }}
            freeSolo
            id="global-search"
            disablePortal
            className="global-search-autocomplete"
            sx={{
              "& + .MuiAutocomplete-popper .MuiAutocomplete-option": {
                backgroundColor: "white"
              },
              "& + .MuiAutocomplete-popper .MuiAutocomplete-option:hover": {
                backgroundColor: "#c7e3ff"
              }
            }}
            value={value}
            onChange={(_event: any, newValue: any) => {
              if (newValue) {
                // Only update options if newValue is a valid option object
                if (typeof newValue === "object" && newValue.title) {
                  setOptions([newValue, ...options]);
                }
                setValue(newValue);
                // Only call handleValues if newValue is a valid option
                if (typeof newValue === "object" && newValue.title && newValue.title !== "undefined") {
                  handleValues(newValue);
                }
              } else {
                setValue("");
              }
            }}
            clearOnBlur={false}
            autoComplete={true}
            includeInputInList
            noOptionsText={"No Entities"}
            disableClearable
            onInputChange={onInputChange}
            getOptionLabel={(option: string | QuickSearchOptionListType) => {
              if (typeof option === "string") {
                return option;
              }
              // Safely extract title, defaulting to empty string if undefined
              const title = (option as any)?.title;
              return title && typeof title === "string" ? title : "";
            }}
            renderOption={(props, option, { inputValue }) => {
              const { entityObj, types, parent } =
                typeof option !== "string" &&
                "entityObj" in option &&
                "types" in option &&
                "parent" in option
                  ? (option as {
                      entityObj: { status?: string; guid?: string };
                      types: string;
                      parent: string;
                    })
                  : { entityObj: null, types: "", parent: "" };
              typeof option !== "string" &&
              "entityObj" in option &&
              "types" in option
                ? option
                : { entityObj: null, types: "", parent: "" };
              const title =
                typeof option !== "string" && "title" in option
                  ? option.title
                  : option;
              
              // Validate and sanitize title to prevent XSS
              const safeTitle = typeof title === "string" ? title : "";
              
              // Validate guid to prevent XSS in URL
              const guid = (entityObj as { guid?: string })?.guid;
              const safeGuid = guid && typeof guid === "string" ? guid : "";
              const href = safeGuid ? `/detailPage/${safeGuid}` : "#";
              
              const { name }: { name: string; found: boolean; key: any } =
                extractKeyValueFromEntity(entityObj);
              
              // Safely handle name extraction
              const safeName = name && typeof name === "string" ? name : "";
              
              const matches = match(
                types === "Entities" ? safeName : safeTitle,
                inputValue || "",
                {
                  findAllOccurrences: true,
                  insideWords: true
                }
              );
              const parts = parse(
                types === "Entities" ? safeName : safeTitle,
                matches
              );
              return (
                <Stack
                  flexDirection="row"
                  component="li"
                  className="global-search-options"
                  sx={{
                    "& > span": {
                      mr: 2,
                      flexShrink: 0
                    }
                  }}
                  {...props}
                  onClick={() => {
                    if (typeof option !== "string") {
                      handleValues(option as unknown as HandleValuesType);
                    }
                  }}
                >
                  {types === "Entities" && !isEmpty(entityObj) ? (
                    <Link
                      className="entity-name text-decoration-none"
                      style={{
                        maxWidth: "100%",
                        width: "100%",
                        color: "black",
                        textDecoration: "none",
                        display: "inline-flex",
                        alignItems: "center",
                        flexWrap: "wrap"
                      }}
                      to={{
                        pathname: href
                      }}
                      color={
                        entityObj?.status &&
                        entityStateReadOnly[entityObj.status]
                          ? "error"
                          : "primary"
                      }
                    >
                      {" "}
                      {types === "Entities" && !isEmpty(entityObj) && (
                        <DisplayImage entity={entityObj} />
                      )}{" "}
                      {types === "Entities" && !isEmpty(entityObj)
                        ? parts.map((part, index) => (
                            <Stack
                              flexDirection="row"
                              key={index}
                              style={{
                                fontWeight: part.highlight ? "bold" : "regular"
                              }}
                            >
                              {entityObj?.guid !== "-1" && !part.highlight ? (
                                <Link
                                  className="entity-name text-blue text-decoration-none"
                                  style={{
                                    color: "black",
                                    textDecoration: "none",
                                    maxWidth: "100%",
                                    width: "100%"
                                  }}
                                  to={{
                                    pathname: href
                                  }}
                                  color={
                                    entityObj?.status &&
                                    entityStateReadOnly[entityObj.status]
                                      ? "error"
                                      : "primary"
                                  }
                                >
                                  {part.text}
                                </Link>
                              ) : (
                                part.text
                              )}
                            </Stack>
                          ))
                        : parts.map((part, index) => (
                            <Stack
                              flexDirection="row"
                              key={index}
                              style={{
                                fontWeight: part.highlight ? "bold" : "regular"
                              }}
                            >
                              {part.text}
                            </Stack>
                          ))}
                      {types === "Entities" &&
                        !isEmpty(entityObj) &&
                        ` (${parent})`}
                    </Link>
                  ) : (
                    parts.map((part, index) => (
                      <Typography
                        component="p"
                        key={index}
                        className="global-search-options-text"
                        sx={{
                          fontWeight: part.highlight ? "bold" : "regular"
                        }}
                      >
                        {" "}
                        {part.text}
                      </Typography>
                    ))
                  )}
                </Stack>
              );
            }}
            renderInput={(params) => (
              <TextField
                {...params}
                placeholder="Search Entities..."
                fullWidth
                onClick={() => {
                  setOpen(true);
                }}
                className="text-black-default"
                InputProps={{
                  style: {
                    padding: "1px 10px",
                    borderRadius: "4px",
                    color: "white !important",
                    opacity: 1
                  },
                  ...params.InputProps,
                  type: "search",
                  endAdornment: (
                    <InputAdornment position="start">
                      {loading ? (
                        <CircularProgress sx={{ color: "gray" }} size={15} />
                      ) : (
                        <SearchIcon fontSize="small" />
                      )}
                    </InputAdornment>
                  )
                }}
              />
            )}
            groupBy={(option) =>
              typeof option !== "string" && "types" in option
                ? String(option.types)
                : ""
            }
            options={options}
            filterOptions={(options) => options}
          />
        </ClickAwayListener>

        <CustomButton
          variant="outlined"
          size="small"
          sx={{
            backgroundColor: "white !important",
            color: "#4a90e2 !important",
            borderColor: "#dddddd !important",
            "&:hover": {
              backgroundColor: "rgba(74, 144, 226, 0.08) !important",
              // borderColor: "#4a90e2 !important",
              color: "#4a90e2 !important"
            }
          }}
          onClick={() => {
            setOpenAdvanceSearch(true);
          }}
        >
          <Typography
            sx={{
              color: "#4a90e2 !important",
              fontWeight: "600 !important",
              fontSize: "0.875rem !important"
            }}
            display="inline"
          >
            Advanced
          </Typography>
        </CustomButton>
      </Stack>

      {openAdvanceSearch && (
        <AdvancedSearch
          openAdvanceSearch={openAdvanceSearch}
          handleCloseModal={handleCloseModal}
        />
      )}
    </>
  );
};

export default QuickSearch;
