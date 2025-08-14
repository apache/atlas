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

import { useEffect, useRef, useState } from "react";
import { ArrowDropDownOutlined } from "@mui/icons-material";
import { AutorenewIcon, CustomButton, LightTooltip } from "../muiComponents";
import {
  CircularProgress,
  List,
  Menu,
  MenuProps,
  Stack,
  Typography,
  alpha,
  styled
} from "@mui/material";
import SaveOutlinedIcon from "@mui/icons-material/SaveOutlined";
import AddIcon from "@mui/icons-material/Add";
import DownloadOutlinedIcon from "@mui/icons-material/DownloadOutlined";
import KeyboardArrowRightOutlinedIcon from "@mui/icons-material/KeyboardArrowRightOutlined";
import KeyboardArrowDownOutlinedIcon from "@mui/icons-material/KeyboardArrowDownOutlined";
import { useLocation } from "react-router-dom";
import { useNavigate } from "react-router-dom";
import { globalSearchParams, isEmpty, serverError } from "@utils/Utils";
import CustomModal from "../Modal";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";
import EntityForm from "@views/Entity/EntityForm";
import SaveFilters from "@views/SaveFilters/SaveFilters";
import { useAppSelector } from "@hooks/reducerHook";
import AddTag from "@views/Classification/AddTag";
import Filters from "@components/QueryBuilder/Filters";
import { attributeFilter } from "@utils/CommonViewFunction";
import { toast } from "react-toastify";
import { downloadSearchResultsCSV } from "@api/apiMethods/downloadApiMethod";
import AssignTerm from "@views/Glossary/AssignTerm";

export const StyledMenu = styled((props: MenuProps) => (
  <div style={{ position: "relative" }}>
    <Menu
      anchorOrigin={{
        vertical: "bottom",
        horizontal: "right"
      }}
      transformOrigin={{
        vertical: "top",
        horizontal: "right"
      }}
      {...props}
    />
  </div>
))(({ theme }: any) => ({
  "& .MuiPaper-root": {
    transitionDelay: "90ms !important",
    opacity: "0",
    borderRadius: 4,
    maxHeight: "300px",
    marginTop: theme.spacing(1),
    minWidth: 180,
    border: "solid 1px #bbb",
    boxShadow: "#999999 0 1px 3px",
    color:
      theme.palette.mode === "light"
        ? "rgb(55, 65, 81)"
        : theme.palette.grey[300],
    "& .MuiMenuItem-root": {
      "& .MuiSvgIcon-root": {
        fontSize: 18,
        color: theme.palette.text.secondary,
        marginRight: theme.spacing(1.5)
      },
      "&:active": {
        backgroundColor: alpha(
          theme.palette.primary.main,
          theme.palette.action.selectedOpacity
        )
      }
    }
  }
}));

export const TableFilter = ({
  getAllColumns,
  defaultColumnParams,
  columnVisibility,
  refreshTable,
  rowSelection,
  setRowSelection,
  queryBuilder,
  allTableFilters,
  columnVisibilityParams,
  setUpdateTable,
  getSelectedRowModel,
  memoizedData
}: any) => {
  const [open, setOpen] = useState<null | HTMLElement>(null);
  const location = useLocation();
  const navigate = useNavigate();
  const searchParams = new URLSearchParams(location.search);
  const searchType = searchParams.get("searchType");
  const toastId: any = useRef(null);
  const [openModal, setOpenModal] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [entityModal, setEntityModal] = useState<boolean>(false);
  const [filtersPopover, setFiltersPopover] =
    useState<HTMLButtonElement | null>(null);
  const filtersOpen = Boolean(filtersPopover);
  const popoverId = filtersOpen ? "simple-popover" : undefined;
  const [_searchState, setSearchState] = useState(
    Object.fromEntries(searchParams)
  );
  const [savedSearchModal, setSavedSearchModal] = useState(false);
  const { sessionObj = "" }: any = useAppSelector(
    (state: any) => state.session
  );
  const [tagModal, setTagModal] = useState<boolean>(false);
  const [termModal, setTermModal] = useState<boolean>(false);

  const { data } = sessionObj || {};
  const key = "atlas.entity.create.allowed";

  const entityCreate = data?.[key] || "";

  const selectedRow = !isEmpty(getSelectedRowModel()?.rows)
    ? getSelectedRowModel()?.rows?.map((obj: { original: any }) => {
        return obj.original;
      })
    : [];

  useEffect(() => {
    setSearchState(Object.fromEntries(searchParams));
  }, [location.search]);

  const handleCloseModal = () => {
    setOpenModal(false);
  };

  const handleCloseEntityModal = () => {
    setEntityModal(false);
  };

  const handleCloseSavedSearchModal = () => {
    setSavedSearchModal(false);
  };

  const handleOpenEntityModal = () => {
    setEntityModal(true);
  };
  const handleOpenModal = () => {
    setOpenModal(true);
  };

  const handleRemove = () => {
    navigate({ pathname: "/search" }, { replace: true });
  };

  const updateSearchParams = (e: Event | any, newParams: any) => {
    let defaultColumnParamsVal = defaultColumnParams;
    let currentAttributes =
      searchParams.get("attributes") || defaultColumnParamsVal;

    const newValue = newParams.attribute.id;
    if (e.target.checked) {
      if (!currentAttributes.includes(newValue)) {
        currentAttributes += currentAttributes ? `,${newValue}` : newValue;
        currentAttributes = currentAttributes.split(",").sort().join(",");
      }
    } else {
      let array = currentAttributes.split(",").sort();

      array = array.filter((item: any) => item !== newValue);

      currentAttributes = array.join(",");
    }

    if (currentAttributes != "") {
      searchParams.set("attributes", currentAttributes);
    } else {
      searchParams.delete("attributes");
    }
    navigate({ search: searchParams.toString() });
  };

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setOpen(event.currentTarget);
  };

  const handleClose = () => {
    setOpen(null);
  };
  const handleClickFilterPopover = (
    event: React.MouseEvent<HTMLButtonElement>
  ) => {
    setFiltersPopover(event.currentTarget);
  };

  const handleCloseTagModal = () => {
    setTagModal(false);
  };

  const handleCloseTermModal = () => {
    setTermModal(false);
  };

  const handleCloseFilterPopover = () => {
    setFiltersPopover(null);
  };

  let ruleUrl = searchParams.get("entityFilters");
  const getIdFromRuleObj = (rule: string | null): any => {
    let col = new Set();

    let ruleObj: any = !isEmpty(rule)
      ? attributeFilter.generateAPIObj(rule)
      : {};

    if (isEmpty(ruleObj?.criterion)) {
      return col;
    }

    for (const obj of ruleObj?.criterion) {
      if (obj.hasOwnProperty("criterion")) {
        return getIdFromRuleObj(obj);
      } else {
        return col.add(obj.attributeName);
      }
    }

    return col;
  };
  const allColumns = !isEmpty(ruleUrl)
    ? getAllColumns().map(
        (obj: { id: unknown; columnDef: { show: boolean } }) => {
          if (Array.from(getIdFromRuleObj(ruleUrl)).includes(obj.id)) {
            obj.columnDef.show = true;
          }
          return obj;
        }
      )
    : getAllColumns();
  const generateAttributeLabelMap = (downloadSearchResultsParams: any) => {
    let params: any = downloadSearchResultsParams;
    let tableColumnsLabelMap = getAllColumns();
    if (!isEmpty(params.searchParameters.attributes)) {
      params.searchParameters.attributes.map((attr: any) => {
        for (let label of tableColumnsLabelMap) {
          if (attr === label.id) {
            params.attributeLabelMap[label.id] = label.columnDef.header;
          }
        }
      });
    }
  };

  const onDownloadSearchResults = async () => {
    const { basicParams, dslParams } = globalSearchParams;
    let downloadSearchResultsParams: any = {
      attributeLabelMap: {},
      searchParameters: {}
    };

    downloadSearchResultsParams.searchParameters = basicParams;

    if (searchType != "basic") {
      downloadSearchResultsParams.searchParameters = dslParams;
      delete downloadSearchResultsParams.attributeLabelMap;
    }
    generateAttributeLabelMap(downloadSearchResultsParams);

    try {
      setLoading(true);
      await downloadSearchResultsCSV(searchType, downloadSearchResultsParams);
      setLoading(false);
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        "The current search results have been enqueued for download. You can access the csv file by clicking the large arrow icon at the top of the page."
      );
    } catch (error) {
      setLoading(false);
      console.error(`Error occur while creating CSV file`, error);
      serverError(error, toastId);
    }
  };

  return (
    <>
      <Stack direction="row" justifyContent={"space-between"}>
        <Stack direction="row" spacing={1}>
          {allTableFilters && (
            <CustomButton
              variant="outlined"
              size="small"
              onClick={(e: any) => {
                e.stopPropagation();
                refreshTable();
              }}
              data-cy="refreshSearchResult"
            >
              <AutorenewIcon className="table-filter-refresh" />
            </CustomButton>
          )}
          {(allTableFilters || queryBuilder) && (
            <CustomButton
              variant="outlined"
              size="small"
              onClick={handleClickFilterPopover}
              startIcon={
                !filtersPopover ? (
                  <KeyboardArrowRightOutlinedIcon />
                ) : (
                  <KeyboardArrowDownOutlinedIcon />
                )
              }
            >
              Filters
            </CustomButton>
          )}
          {allTableFilters && (
            <CustomButton
              variant="outlined"
              size="small"
              onClick={() => {
                handleOpenModal();
              }}
            >
              Clear
            </CustomButton>
          )}
        </Stack>
        <Stack direction="row" spacing={1}>
          {!isEmpty(rowSelection) && (
            <>
              <CustomButton
                variant="outlined"
                size="small"
                onClick={(_e: any) => {
                  setTermModal(true);
                }}
                startIcon={<AddIcon />}
              >
                Term
              </CustomButton>
              <CustomButton
                variant="outlined"
                size="small"
                onClick={(_e: any) => {
                  setTagModal(true);
                }}
                startIcon={<AddIcon />}
              >
                Classification
              </CustomButton>
            </>
          )}
          {allTableFilters && (
            <LightTooltip title="Save as custom filter">
              <CustomButton
                variant="outlined"
                size="small"
                onClick={(_e: any) => {
                  setSavedSearchModal(true);
                }}
                startIcon={<SaveOutlinedIcon />}
              >
                Save Filter
              </CustomButton>
            </LightTooltip>
          )}

          {columnVisibility && memoizedData?.length > 0 && (
            <>
              {" "}
              <CustomButton
                variant="outlined"
                // classes="table-filter-btn"
                size="small"
                onClick={(e: any) => handleClick(e)}
                endIcon={<ArrowDropDownOutlined />}
                data-cy="colManager"
              >
                Columns
              </CustomButton>
              <StyledMenu
                anchorEl={open}
                open={Boolean(open)}
                onClose={handleClose}
              >
                <List
                  className="table-filter-column-list"
                  sx={{
                    fontSize: "14px !important",
                    transition: "none !important"
                  }}
                >
                  {allColumns.map((column: any) => {
                    if (column.id === "select") {
                      return null;
                    }

                    return (
                      <div key={column.id} className="px-1 table-filter-column">
                        <label>
                          <input
                            {...{
                              type: "checkbox",
                              checked: column.getIsVisible(),
                              onChange: column.getToggleVisibilityHandler(),
                              onClick: (e) => {
                                if (columnVisibilityParams) {
                                  updateSearchParams(e, { attribute: column });
                                }
                              }
                            }}
                          />
                          {column.columnDef.header}
                        </label>
                      </div>
                    );
                  })}
                </List>
              </StyledMenu>
              {/* <LabelPicker
                anchorEl={anchorEl}
                id={"label-picker"}
                value={filterValue}
                handleCloseLabelPicker={handleCloseLabelPicker}
                pendingValue={pendingValue}
                setPendingValue={setPendingValue}
                handleClickLabelPicker={handleClickLabelPicker}
                Label={
                  <LightTooltip title="Type Filter">
                    <FilterAltTwoToneIcon
                      color="primary"
                      className="label-picker-filter-icon"
                    />
                  </LightTooltip>
                }
                optionList={filterLabelData}
              /> */}
            </>
          )}

          {allTableFilters && memoizedData?.length > 0 && (
            <CustomButton
              variant="outlined"
              className={loading ? "cursor-not-allowed" : "cursor-pointer"}
              size="small"
              onClick={(_e: any) => onDownloadSearchResults()}
              startIcon={
                loading ? (
                  <CircularProgress size="small" color="inherit" />
                ) : (
                  <DownloadOutlinedIcon />
                )
              }
              disabled={loading}
            >
              Download
            </CustomButton>
          )}
          {entityCreate &&
            allTableFilters &&
            searchParams.get("relationshipName") == undefined && (
              <CustomButton
                variant="contained"
                size="small"
                onClick={(_e: any) => handleOpenEntityModal()}
                startIcon={<AddIcon />}
              >
                Create Entity
              </CustomButton>
            )}
        </Stack>
      </Stack>

      {entityModal && (
        <EntityForm open={entityModal} onClose={handleCloseEntityModal} />
      )}
      {savedSearchModal && (
        <SaveFilters
          open={savedSearchModal}
          onClose={handleCloseSavedSearchModal}
        />
      )}

      {termModal && (
        <AssignTerm
          open={termModal}
          columnVal={"terms"}
          onClose={handleCloseTermModal}
          data={selectedRow}
          relatedTerm={undefined}
          updateTable={setUpdateTable}
          setRowSelection={setRowSelection}
        />
      )}

      {tagModal && (
        <AddTag
          open={tagModal}
          isAdd={true}
          entityData={selectedRow}
          onClose={handleCloseTagModal}
          setUpdateTable={setUpdateTable}
          setRowSelection={setRowSelection}
        />
      )}

      {filtersOpen && (
        <Filters
          popoverId={popoverId}
          filtersOpen={filtersOpen}
          filtersPopover={filtersPopover}
          handleCloseFilterPopover={handleCloseFilterPopover}
          setUpdateTable={setUpdateTable}
        />
      )}
      {openModal && (
        <div style={{ position: "absolute" }}>
          <CustomModal
            open={openModal}
            onClose={handleCloseModal}
            title="Confirmation"
            titleIcon={
              <ErrorRoundedIcon
                fontSize="small"
                className="remove-modal-icon"
              />
            }
            button1Label="Cancel"
            button1Handler={handleCloseModal}
            button2Label="Remove"
            button2Handler={handleRemove}
          >
            <Typography fontSize={15}>
              Search parameters will be reset and you will return to the default
              search page. Continue?
            </Typography>{" "}
          </CustomModal>
        </div>
      )}
    </>
  );
};

export default TableFilter;
