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
  Button,
  InputAdornment,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import { useState } from "react";
import { getGlobalSearchResult } from "../../api/apiMethods/searchApiMethod";
import DisplayImage from "../EntityDisplayImage";
import SearchIcon from "@mui/icons-material/Search";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { entityStateReadOnly } from "../../utils/Enum";
import { extractKeyValueFromEntity, isEmpty } from "../../utils/Utils";
import parse from "autosuggest-highlight/parse";
import match from "autosuggest-highlight/match";
import { ClickAwayListener } from "@mui/base/ClickAwayListener";
import AdvancedSearch from "./AdvancedSearch";
import {
  HandleValuesType,
  QuickSearchOptionListType,
  QuickSearchValueType,
  SuggestionDataType
} from "../../models/globalSearchType";
import { AxiosResponse } from "axios";

const QuickSearch = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const [value, setValue] = useState<string | QuickSearchValueType | null>(
    null
  );
  const [options, setOptions] = useState<any>([]);
  const [open, setOpen] = useState<boolean>(false);
  const [openAdvanceSearch, setOpenAdvanceSearch] = useState<boolean>(false);

  const getData = async (searchTerm: string) => {
    let entities: QuickSearchOptionListType = [];
    let suggestionNames: QuickSearchOptionListType = [];
    let quickSearchResp: AxiosResponse = await getGlobalSearchResult("quick", {
      params: { query: searchTerm, limit: 5, offset: 0 }
    });
    let suggestionSearchResp: AxiosResponse = await getGlobalSearchResult(
      "suggestions",
      {
        params: { prefixString: searchTerm }
      }
    );
    const { searchResults } = quickSearchResp.data;
    const { suggestions }: SuggestionDataType = suggestionSearchResp.data;

    entities = !isEmpty(searchResults?.entities)
      ? searchResults?.entities?.map((entityDef: any) => {
          return {
            title: `${entityDef.displayText}`,
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
    if (value) {
      setOpen(true);
      getData(value);
    } else {
      setOptions([]);
      setValue(null);
      setOpen(false);
    }
  };

  const handleValues = (option: HandleValuesType) => {
    const { entityObj, title, types } = option;
    setOpen(false);
    types == "Suggestions" && setValue({ title: title });
    searchParams.set("query", title);
    searchParams.set("searchType", "basic");

    types == "Entities"
      ? navigate(
          {
            pathname: `/detailPage/${entityObj.guid}`
          },
          { replace: true }
        )
      : navigate(
          {
            pathname: `/search/searchResult`,
            search: searchParams.toString()
          },
          { replace: true }
        );
  };

  const handleClickAway = () => {
    setOpen(false);
  };

  const handleCloseModal = () => {
    setOpenAdvanceSearch(false);
  };

  return (
    <>
      <Stack direction="row" className="global-search-stack">
        <ClickAwayListener onClickAway={handleClickAway}>
          <Autocomplete
            slotProps={{
              paper: {
                sx: {
                  "& .MuiAutocomplete-listbox": {
                    "& .MuiAutocomplete-option": {
                      backgroundColor: "#f6f6f6"
                    }
                  }
                }
              }
            }}
            open={open}
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
            clearOnBlur={false}
            autoComplete={true}
            includeInputInList
            onChange={(_event: any, newValue: any) => {
              setOptions(newValue ? [newValue, ...options] : options);
              setValue(newValue);
              handleValues(newValue);
            }}
            noOptionsText={"No Entities"}
            disableClearable
            onInputChange={onInputChange}
            getOptionLabel={(option) => option.title}
            renderOption={(props, option, { inputValue }) => {
              const { entityObj, title, types, parent } = option;
              const href = `/detailPage/${entityObj?.guid}`;
              const { name }: { name: string; found: boolean; key: any } =
                extractKeyValueFromEntity(entityObj);
              const matches = match(
                types == "Entities" ? name : title,
                inputValue,
                {
                  findAllOccurrences: true,
                  insideWords: true
                }
              );
              const parts = parse(types == "Entities" ? name : title, matches);
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
                  key={types == "Entities" ? name : title}
                  onClick={() => {
                    handleValues(option);
                  }}
                >
                  {types == "Entities" && !isEmpty(entityObj) ? (
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
                      {types == "Entities" && !isEmpty(entityObj) && (
                        <DisplayImage entity={entityObj} />
                      )}{" "}
                      {types == "Entities" && !isEmpty(entityObj)
                        ? parts.map((part, index) => (
                            <Stack
                              flexDirection="row"
                              key={index}
                              style={{
                                fontWeight: part.highlight ? "bold" : "regular"
                              }}
                            >
                              {entityObj?.guid != "-1" && !part.highlight ? (
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
                      {types == "Entities" &&
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
                      <SearchIcon fontSize="small" />
                    </InputAdornment>
                  )
                }}
              />
            )}
            groupBy={(option) => option.types}
            options={options}
          />
        </ClickAwayListener>

        <Button
          onClick={() => {
            setOpenAdvanceSearch(true);
          }}
          className="advanced-search-link cursor-pointer"
        >
          <Typography
            className="advanced-search-label"
            sx={{
              color: location?.pathname == "/search" ? "blue" : "eeeeee"
            }}
            display="inline"
          >
            Advanced
          </Typography>
        </Button>
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
