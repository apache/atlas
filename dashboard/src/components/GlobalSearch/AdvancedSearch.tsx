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
  Divider,
  FormControl,
  Grid,
  IconButton,
  List,
  ListItem,
  ListItemText,
  Stack,
  styled,
  TextField,
  Tooltip,
  tooltipClasses,
  TooltipProps,
  Typography
} from "@mui/material";
import { AutorenewIcon, CustomButton, LightTooltip } from "../muiComponents";
import { useEffect, useMemo, useRef, useState } from "react";
import { useAppDispatch, useAppSelector } from "../../hooks/reducerHook";
import {
  ChildrenInterface,
  ServiceTypeFlatInterface,
  ServiceTypeInterface,
  TypedefHeaderDataType,
  TypeHeaderInterface
} from "../../models/entityTreeType";
import { customSortBy, groupBy, isEmpty } from "../../utils/Utils";
import FilterAltTwoToneIcon from "@mui/icons-material/FilterAltTwoTone";
import LabelPicker from "../LabelPicker";
import CustomModal from "../Modal";
import Link from "@mui/material/Link";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import HelpOutlinedIcon from "@mui/icons-material/HelpOutlined";
import { useLocation, useNavigate } from "react-router-dom";
import RestartAltOutlinedIcon from "@mui/icons-material/RestartAltOutlined";
import { toast } from "react-toastify";
import { AdvanceSearchQueries } from "../../utils/Enum";
import { AdvancedSearchPropsType } from "../../models/globalSearchType";
import { fetchTypeHeaderData } from "@redux/slice/typeDefSlices/typeDefHeaderSlice";

const HtmlTooltip = styled(({ className, ...props }: TooltipProps) => (
  <Tooltip
    {...props}
    classes={{ popper: className }}
    arrow
    placement="bottom-start"
  />
))(({ theme }) => ({
  [`& .${tooltipClasses.tooltip}`]: {
    backgroundColor: "#f5f5f9",
    color: "rgba(0, 0, 0, 0.87)",
    maxWidth: 500,
    fontSize: theme.typography.pxToRem(12),
    border: "1px solid #dadde9"
  }
}));

const AdvancedSearch: React.FC<AdvancedSearchPropsType> = ({
  openAdvanceSearch,
  handleCloseModal
}) => {
  const navigate = useNavigate();
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const searchType = searchParams.get("searchType");
  const typeValueParam = !isEmpty(searchType) ? searchParams.get("type") : "";
  const queryParam = !isEmpty(searchType) ? searchParams.get("query") : "";
  const dispatch = useAppDispatch();
  const toastId: any = useRef(null);
  const { typeHeaderData }: TypedefHeaderDataType = useAppSelector(
    (state: any) => state.typeHeader
  );
  const { metricsData }: any = useAppSelector((state: any) => state.metrics);
  const [serviceTypeArr, setServiceTypeArr] = useState<ServiceTypeInterface[]>(
    []
  );
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [filterValue, setFilterValue] = useState([]);
  const [pendingValue, setPendingValue] = useState([]);
  const [typeValue, setTypeValue] = useState<any>(
    !isEmpty(typeValueParam) && searchType == "dsl" ? typeValueParam : ""
  );
  const [queryValue, setQueryValue] = useState<any>(
    !isEmpty(queryParam) && searchType == "dsl" ? queryParam : ""
  );

  useEffect(() => {
    if (typeHeaderData) {
      const newServiceTypeArr: ServiceTypeInterface[] = [];

      typeHeaderData.forEach((entity: TypeHeaderInterface) => {
        let { serviceType = "other_types", category, name, guid } = entity;
        let entityCount = 0;
        let modelName = "";
        let children: ChildrenInterface = {
          gType: "",
          guid: "",
          id: "",
          name: "",
          type: "",
          text: ""
        };

        if (
          category === "ENTITY" &&
          metricsData &&
          !isEmpty(metricsData.data)
        ) {
          entityCount =
            (metricsData.data.entity.entityActive[name] || 0) +
            (metricsData.data.entity.entityDeleted[name] || 0);
          modelName = entityCount ? `${name} (${entityCount})` : name;
          children = {
            text: modelName,
            name: name,
            type: category,
            gType: "Entity",
            guid: guid,
            id: guid,
            serviceType: serviceType.toUpperCase()
          };

          generateServiceTypeArr(newServiceTypeArr, children);
        }
      });

      setServiceTypeArr(newServiceTypeArr);
    }
  }, [typeHeaderData, metricsData]);
  const fetchInitialData = async () => {
    await dispatch(fetchTypeHeaderData());
  };
  const generateServiceTypeArr = (
    entityCountArr: ServiceTypeInterface[],

    children: ChildrenInterface | any
  ) => {
    return entityCountArr.push(children);
  };

  const treeData = useMemo(() => {
    return customSortBy(
      serviceTypeArr as unknown as ServiceTypeFlatInterface[],
      ["name"]
    );
  }, [serviceTypeArr]);

  const searchTypeData = useMemo(() => {
    if (isEmpty(pendingValue)) {
      return treeData;
    } else {
      return !isEmpty(treeData)
        ? pendingValue
            .map((obj) => groupBy(treeData, "serviceType")[obj])
            .flat()
        : [];
    }
  }, [pendingValue, treeData]);

  const filterLabelData = Object.keys(groupBy(treeData, "serviceType"));

  const handleCloseLabelPicker = () => {
    setFilterValue(pendingValue);
    if (anchorEl) {
      anchorEl.focus();
    }
    setAnchorEl(null);
  };

  const handleClickLabelPicker = (event: React.MouseEvent<HTMLElement>) => {
    setPendingValue(filterValue);
    setAnchorEl(event?.currentTarget);
  };

  const handleSearch = () => {
    if (typeValue == "" && queryValue == "") {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please Select any value");
      return;
    }

    const filterValue = treeData.find((obj: { text: string }) => {
      if (obj.text == typeValue) {
        return obj;
      }
    });
    queryValue != undefined &&
      queryValue != "" &&
      searchParams.set("query", queryValue);
    filterValue != undefined &&
      filterValue != "" &&
      searchParams.set("type", filterValue.name);
    if (!isEmpty(queryValue) || !isEmpty(filterValue)) {
      searchParams.set("searchType", "dsl");
      searchParams.set("dslChecked", "true");
      navigate(
        {
          pathname:
            location?.pathname == "/search"
              ? `searchresult`
              : "/search/searchResult",
          search: searchParams.toString()
        },
        { replace: true }
      );

      handleCloseModal();
    }
  };

  const handleClearValue = () => {
    setTypeValue("");
    setQueryValue("");
  };

  return (
    <>
      <CustomModal
        open={openAdvanceSearch}
        onClose={handleCloseModal}
        title="Advanced Search"
        postTitleIcon={
          <>
            <HtmlTooltip
              arrow
              title={
                <>
                  <Typography className="advanced-search-title">
                    Advanced Search Queries
                  </Typography>
                  <Divider />
                  <br />
                  <Typography color="inherit">
                    Use DSL (Domain Specific Language) to build queries
                  </Typography>
                  <Grid container>
                    <Grid>
                      <List className="advanced-search-queries-list">
                        {AdvanceSearchQueries.map((query) => {
                          return (
                            <ListItem className="advanced-search-queries-listitem">
                              <ListItemText
                                primary={query.type}
                                secondary={query.queries}
                              />
                            </ListItem>
                          );
                        })}
                      </List>
                      <Divider />

                      <Link
                        href="http://atlas.apache.org/#/SearchAdvance"
                        rel="noopener"
                        target="_blank"
                        color="inherit"
                        underline="none"
                      >
                        <Typography className="text-color-green more-sample-queries cursor-pointer">
                          <InfoOutlinedIcon /> More sample queries and use-cases{" "}
                        </Typography>
                      </Link>
                    </Grid>
                  </Grid>
                </>
              }
            >
              <HelpOutlinedIcon />
            </HtmlTooltip>
          </>
        }
        button1Label="Cancel"
        button1Handler={handleCloseModal}
        button2Label="Search"
        button2Handler={handleSearch}
      >
        <Stack flexDirection="row" gap="4px" marginBottom="1.25rem">
          <LabelPicker
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
          />

          <FormControl size="small" className="advanced-search-formcontrol">
            <Autocomplete
              size="small"
              value={typeValue}
              onChange={(_event: any, newValue: string | null) => {
                setTypeValue(newValue);
              }}
              disableClearable
              id="search-by-type"
              options={searchTypeData.map(
                (option: { text: string }) => option.text
              )}
              className="advanced-search-autocomplete"
              renderInput={(params) => (
                <TextField
                  {...params}
                  fullWidth
                  onChange={(event: any) => {
                    setTypeValue(event.target.value);
                  }}
                  label="Search By Type"
                  InputLabelProps={{
                    style: {
                      top: "unset",
                      bottom: "16px"
                    }
                  }}
                  InputProps={{
                    style: {
                      padding: "0px 32px 0px 4px",
                      height: "36px",
                      lineHeight: "1.2"
                    },
                    ...params.InputProps,
                    type: "search"
                  }}
                />
              )}
            />
          </FormControl>
          <IconButton
            aria-label="Refresh"
            size="small"
            className="cursor-pointer advanced-search-refresh-btn"
            onClick={() => {
              fetchInitialData();
            }}
          >
            <LightTooltip title="Refresh">
              <AutorenewIcon
                fontSize="inherit"
                color="success"
                className="advanced-search-refresh-icon"
              />
            </LightTooltip>
          </IconButton>
        </Stack>
        <Stack paddingBottom="1.5rem" maxWidth="100%">
          <TextField
            onClick={(e: any) => {
              e.stopPropagation();
              e.isDefaultPrevented();
            }}
            value={queryValue}
            onChange={(e) => {
              setQueryValue(e.target.value);
            }}
            sx={{
              height: "36px",
              "& .MuiOutlinedInput-root": { height: "36px", padding: 0 },
              "& .MuiInputLabel-root": { bottom: "16px", top: "unset" }
            }}
            fullWidth
            placeholder={`Search By Query eg. where name="sales_fact"`}
            label="Search By Query"
            size="small"
          />
        </Stack>
        <CustomButton
          variant="outlined"
          color="success"
          aria-label="reset"
          primary={true}
          size="small"
          onClick={(e: Event) => {
            e.stopPropagation();
            handleClearValue();
          }}
          startIcon={<RestartAltOutlinedIcon />}
        >
          Reset
        </CustomButton>
      </CustomModal>
    </>
  );
};

export default AdvancedSearch;
