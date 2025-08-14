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

//@ts-nocheck

import { MouseEvent, ChangeEvent, useEffect, useRef, useState } from "react";
import {
  addLineageData,
  getLineageData
} from "@api/apiMethods/lineageMethod.js";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import { extractKeyValueFromEntity, isEmpty } from "@utils/Utils";
import {
  Autocomplete,
  Box,
  Button,
  CircularProgress,
  Collapse,
  createFilterOptions,
  Divider,
  Drawer,
  FormControl,
  IconButton,
  MenuItem,
  Popover,
  Slide,
  Stack,
  Switch,
  TextField,
  Typography
} from "@mui/material";
import LineageHelper from "@views/Lineage/atlas-lineage/src";
import { useSelector } from "react-redux";
import {
  Close as CloseIcon,
  Search as SearchIcon,
  FilterList as FilterIcon,
  Settings as SettingsIcon,
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon,
  Fullscreen as FullscreenIcon,
  CameraAlt as CameraAltIcon,
  Refresh as RefreshIcon
} from "@mui/icons-material";
import { LightTooltip } from "@components/muiComponents";
import { toast } from "react-toastify";
import PropagationPropertyModal from "@views/DetailPage/EntityDetailTabs/PropagationPropertyModal";
import { globalSessionData, lineageDepth } from "@utils/Enum";
import { getValues } from "@components/commonComponents";
import { Link } from "react-router-dom";
import { AntSwitch } from "@utils/Muiutils";

const filter = createFilterOptions();

const LineageTab = ({ entity, isProcess }: any) => {
  const {
    typeName = "",
    attributes,
    guid: entityGuid,
    isIncomplete,
    status,
    classifications
  } = entity || {};
  const { name }: { name: string; found: boolean; key: any } =
    extractKeyValueFromEntity(entity);
  const { isLineageOnDemandEnabled, lineageNodeCount } = globalSessionData;
  let lineageOnDemandPayload = {};
  let relationsOnDemand = {};
  const optionsVal = [
    { label: 3 },
    { label: 6 },
    { label: 9 },
    { label: 12 },
    { label: 15 },
    { label: 18 },
    { label: 21 }
  ];
  let nodeCount = [3, 6, lineageNodeCount];
  let nodeCountArray = [...new Set(nodeCount)];
  const { guid } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const searchParams = new URLSearchParams(location.search);
  let lineageDivRef = useRef(null);
  let legendRef = useRef(null);
  const { entityData = {} } = useSelector((state) => state.entity);
  const [loader, setLoader] = useState(false);
  const [lineageData, setLineageData] = useState([]);
  const [propagationModal, setPropagationModal] = useState<boolean>(false);
  const [lineageMethods, setLineageMethods] = useState();
  const [settingsPopover, setSettingsPopover] = useState(null);
  const [filterPopover, setFilterPopover] = useState(null);
  const [searchPopover, setSearchPopover] = useState(null);
  const [propagateDetails, setPropagateDetails] = useState({});

  const filterObj = useRef({
    isProcessHideCheck: false,
    isDeletedEntityHideCheck: false,
    depthCount: lineageDepth
  });

  const openSettingsPopover = Boolean(settingsPopover);
  const settingPopoverId = openSettingsPopover ? "simple-popover" : undefined;
  const openFiltersPopover = Boolean(filterPopover);
  const filterPopoverId = openSettingsPopover ? "simple-popover" : undefined;
  const openSearchoPover = Boolean(searchPopover);
  const searchPopoverId = searchPopover ? "simple-popover" : undefined;
  const [fullNamechecked, setFullNameChecked] = useState(false);
  const [hideProcess, setHideProcess] = useState(false);
  const [currentPathChecked, setCurrentPathChecked] = useState(true);
  const [checkedDeletedEntity, setCheckedDeletedEntity] = useState(false);
  const [nodeDetailsChecked, setNodeDetailsChecked] = useState(false);
  const [value, setValue] = useState<any>({ label: 3 });
  const [nodeValue, setNodeValue] = useState<any>(3);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [typeValue, setTypeValue] = useState("");
  const [nodeDetails, setNodeDetails] = useState({});
  const [isFullscreen, setIsFullscreen] = useState(false);

  const toggleFullscreen = () => {
    setIsFullscreen(!isFullscreen);
  };

  const handleSettingsPopoverOpen = (
    event: MouseEvent<HTMLButtonElement, MouseEvent>
  ) => {
    setSettingsPopover(event.currentTarget);
  };
  const handleFiltersPopoverOpen = (
    event: MouseEvent<HTMLButtonElement, MouseEvent>
  ) => {
    setFilterPopover(event.currentTarget);
  };
  const handleSearchPopoverOpen = (
    event: MouseEvent<HTMLButtonElement, MouseEvent>
  ) => {
    setSearchPopover(event.currentTarget);
  };
  const handlePopoverClose = () => {
    setSettingsPopover(null);
  };

  const handleFilterPopoverClose = () => {
    setFilterPopover(null);
  };
  const handleSearchPopoverClose = () => {
    setSearchPopover(null);
  };

  let classificationNamesArray: any = [];
  if (classifications) {
    classifications.forEach((item: { typeName: any }) => {
      classificationNamesArray.push(item.typeName);
    });
  }

  let currentEntityData = {
    classificationNames: classificationNamesArray,
    displayText: attributes?.name,
    labels: [],
    meaningNames: [],
    meanings: []
  };

  currentEntityData = Object.assign(currentEntityData, {
    attributes: attributes,
    guid: entityGuid,
    isIncomplete: isIncomplete,
    status: status,
    typeName: typeName
  });
  const generateAddButtonId = (btnType) => {
    return btnType + Math.random().toString(16).slice(2);
  };
  const createExpandButtonObj = (options) => {
    var defaultObj = {
        attributes: {
          owner: "",
          createTime: 0,
          qualifiedName: "PlusBtn",
          name: "PlusBtn",
          description: ""
        },
        isExpandBtn: true,
        classificationNames: [],
        displayText: "Expand",
        isIncomplete: false,
        labels: [],
        meaningNames: [],
        meanings: [],
        status: "ACTIVE",
        typeName: "Table"
      },
      btnObj = Object.assign({}, defaultObj),
      btnType = options.btnType === "Input" ? "more-inputs" : "more-outputs",
      btnId = generateAddButtonId(btnType);
    btnObj.guid = btnId;
    btnObj.parentNodeGuid = options.nodeId;
    btnObj.btnType = options.btnType;
    return btnObj;
  };

  const updateLineageData = (data) => {
    let rawData = data;
    let plusBtnsObj: any = {};
    let plusBtnRelationsArray: any = [];
    relationsOnDemand = rawData.relationsOnDemand
      ? rawData.relationsOnDemand
      : null;

    if (relationsOnDemand) {
      for (const values in relationsOnDemand) {
        if (relationsOnDemand[values].hasMoreInputs) {
          let btnType = "Input";
          let moreInputBtnObj = createExpandButtonObj({
            nodeId: values,
            btnType: btnType
          });
          plusBtnsObj[moreInputBtnObj.guid] = moreInputBtnObj;
          plusBtnRelationsArray.push({
            fromEntityId: moreInputBtnObj.guid,
            toEntityId: values,
            relationshipId: "dummy"
          });
        }
        if (relationsOnDemand[values].hasMoreOutputs) {
          let btnType = "Output";
          let moreOutputBtnObj = createExpandButtonObj({
            nodeId: values,
            btnType: btnType
          });
          plusBtnsObj[moreOutputBtnObj.guid] = moreOutputBtnObj;
          plusBtnRelationsArray.push({
            fromEntityId: values,
            toEntityId: moreOutputBtnObj.guid,
            relationshipId: "dummy"
          });
        }
      }

      return {
        plusBtnsObj: plusBtnsObj,
        plusBtnRelationsArray: plusBtnRelationsArray
      };
    }
  };

  let initialQueryObj = {};
  initialQueryObj[guid] = {
    direction: "BOTH",
    inputRelationsLimit: lineageNodeCount,
    outputRelationsLimit: lineageNodeCount,
    depth: lineageDepth
  };

  const fetchGraph = async (options) => {
    const { queryParam, legends } = options || {};
    const { depth } = queryParam;
    try {
      setLoader(true);
      let response;
      if (isLineageOnDemandEnabled) {
        response = await addLineageData(guid as string, queryParam);
      } else {
        response = await getLineageData(
          guid as string,
          !isEmpty(depth) ? { depth: depth } : {}
        );
      }

      const { data } = response;
      let lineageObj = { ...data };
      lineageObj["legends"] = options ? legends : true;
      lineageOnDemandPayload = lineageObj.lineageOnDemandPayload
        ? lineageObj.lineageOnDemandPayload
        : {};

      if (isEmpty(lineageObj.relations)) {
        if (
          isEmpty(lineageObj.guidEntityMap) ||
          !lineageObj.guidEntityMap[lineageObj.baseEntityGuid]
        ) {
          lineageObj.guidEntityMap[lineageObj.baseEntityGuid] =
            currentEntityData;
        }
      }
      let updatedData = updateLineageData(lineageObj);
      Object.assign(lineageObj.guidEntityMap, updatedData.plusBtnsObj);
      lineageObj.relations = lineageObj.relations.concat(
        updatedData.plusBtnRelationsArray
      );
      setLineageData(lineageObj);
      setDrawerOpen(false);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      console.log("Error while fetching lineage data", error);
    }
  };
  useEffect(() => {
    fetchGraph({
      queryParam: initialQueryObj
    });
  }, [guid]);

  let lineageHelperref = {};

  const createGraph = (data: any) => {
    if (!isEmpty(data)) {
      const LineageHelperRef = new LineageHelper({
        entityDefCollection: entityData?.entityDefs || {},
        data: data,
        el: lineageDivRef.current,
        legendsEl: legendRef.current,
        legends: data.legends,
        getFilterObj: function () {
          const { isProcessHideCheck, isDeletedEntityHideCheck } =
            filterObj.current;
          return {
            isProcessHideCheck: isProcessHideCheck,
            isDeletedEntityHideCheck: isDeletedEntityHideCheck
          };
        },
        isShowHoverPath: function () {
          return currentPathChecked;
        },
        isShowTooltip: function () {
          return nodeDetailsChecked;
        },

        onPathClick: function (d: {
          pathRelationObj: { relationshipId: any };
        }) {
          if (d.pathRelationObj) {
            let relationshipId = d.pathRelationObj.relationshipId;
            setPropagationModal(true);
            setPropagateDetails({
              edgeInfo: d.pathRelationObj,
              relationshipId: relationshipId,
              lineageData: lineageData,
              apiGuid: {}
            });
          }
        },
        onNodeClick: function (d: { clickedData: string | string[] }) {
          if (d.clickedData.indexOf("more") >= 0) {
            onExpandNodeClick({
              guid: d.clickedData
            });
            return;
          }
          // setDrawerOpen(true);
          updateRelationshipDetails(d.clickedData);
        },
        onLabelClick: function (d: { clickedData: any }) {
          var labelGuid = d.clickedData;

          if (labelGuid.indexOf("more") >= 0) {
            return;
          }
          if (guid == labelGuid) {
            toast.info(
              `You are already on ${
                !isEmpty(entity) ? `${name} (${typeName})` : ""
              } detail page`
            );
          } else {
            searchParams.set("tabActive", "properties");
            navigate(
              {
                pathname: `/detailPage/${labelGuid}`,
                search: searchParams.toString()
              },
              { replace: true }
            );
          }
        },
        beforeRender: function () {
          setLoader(true);
        },
        afterRender: function () {
          setLoader(false);
        }
      });

      lineageHelperref = LineageHelperRef;
      setLineageMethods(LineageHelperRef);
    }
  };

  useEffect(() => {
    if (!isEmpty(lineageData)) {
      createGraph(lineageData);
    }
  }, [lineageData, currentPathChecked, nodeDetailsChecked]);

  const {
    zoomIn,
    zoomOut,
    exportLineage,
    displayFullName,
    refresh,
    searchNode
  } = lineageMethods || {};
  const { guidEntityMap } = lineageData || {};

  const onClickResetLineage = () => {
    if (isLineageOnDemandEnabled) {
      fetchGraph({
        queryParam: initialQueryObj,
        legends: false
      });
    }
    if (!isLineageOnDemandEnabled) {
      refresh();
    }

    setFullNameChecked(false);
    displayFullName({
      bLabelFullText: false
    });
  };

  let lineageSearchOptions = !isEmpty(guidEntityMap)
    ? Object.values(guidEntityMap).map((obj) => ({
        label: obj.displayText,
        value: obj.guid
      }))
    : [];
  const onExpandNodeClick = (options) => {
    var parentNodeData = lineageHelperref.getNode(options.guid);
    updateQueryObject(parentNodeData.parentNodeGuid, parentNodeData.btnType);
  };

  const updateRelationshipDetails = (guid) => {
    let initialData = lineageHelperref.getNode(guid);
    if (initialData === undefined) {
      return;
    }
    let typeName = initialData.typeName || guid;
    let attributeDefs =
      initialData && initialData.entityDef
        ? initialData.entityDef.attributeDefs
        : null;
    let config = {
      guid: "guid",
      typeName: "typeName",
      name: "name",
      qualifiedName: "qualifiedName",
      owner: "owner",
      createTime: "createTime",
      status: "status",
      classificationNames: "classifications",
      meanings: "term"
    };
    let data = {};
    Object.entries(config).forEach(([valKey, key]) => {
      var val = initialData[valKey];
      if (
        isEmpty(val) &&
        initialData.attributes &&
        initialData.attributes[valKey]
      ) {
        val = initialData.attributes[valKey];
      }
      if (val) {
        data[valKey] = val;
      }
    });
    setNodeDetails({ valueObject: data, attributeDefs: attributeDefs });
    setDrawerOpen(true);
  };
  const updateQueryObject = (parentId, btnType) => {
    let inputLimit = null;
    let outputLimit = null;
    if (lineageData.lineageOnDemandPayload.hasOwnProperty(parentId)) {
      Object.entries(lineageData.lineageOnDemandPayload).forEach(
        ([key, value]) => {
          if (key === parentId) {
            if (btnType === "Input") {
              value.inputRelationsLimit =
                value.inputRelationsLimit + lineageNodeCount;
              value.outputRelationsLimit = value.outputRelationsLimit;
            }
            if (btnType === "Output") {
              value.inputRelationsLimit = value.inputRelationsLimit;
              value.outputRelationsLimit =
                value.outputRelationsLimit + lineageNodeCount;
            }
          }
        }
      );
    } else {
      var relationCount = validateInputOutputLimit(parentId, btnType);
      if (btnType === "Input") {
        inputLimit = relationCount.inputRelationCount + lineageNodeCount;
        outputLimit = relationCount.outputRelationCount;
      }
      if (btnType === "Output") {
        inputLimit = relationCount.inputRelationCount;
        outputLimit = relationCount.outputRelationCount + lineageNodeCount;
      }
      lineageData.lineageOnDemandPayload[parentId] = {
        direction: "BOTH",
        inputRelationsLimit: inputLimit,
        outputRelationsLimit: outputLimit,
        depth: lineageDepth
      };
    }
    fetchGraph({
      queryParam: lineageData.lineageOnDemandPayload,
      legends: false
    });
  };

  const validateInputOutputLimit = (parentId, btnType) => {
    let inputRelationCount;
    let outputRelationCount;
    for (let guid in lineageData.relationsOnDemand) {
      if (parentId === guid && (btnType === "Input" || btnType === "Output")) {
        inputRelationCount = lineageData.relationsOnDemand[guid]
          .inputRelationsCount
          ? lineageData.relationsOnDemand[guid].inputRelationsCount
          : lineageNodeCount;
        outputRelationCount = lineageData.relationsOnDemand[guid]
          .outputRelationsCount
          ? lineageData.relationsOnDemand[guid].outputRelationsCount
          : lineageNodeCount;
      }
    }
    return {
      inputRelationCount: inputRelationCount,
      outputRelationCount: outputRelationCount
    };
  };

  const fullNameHandleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    event.stopPropagation();
    setFullNameChecked(event.target.checked);
    displayFullName({
      bLabelFullText: event.target.checked == true ? true : false
    });
  };

  const currentPathHandleChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    setCurrentPathChecked(event.target.checked);
  };

  const handleHideProcessChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    setHideProcess(event.target.checked);
  };

  const handleDeletedEntityChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    setCheckedDeletedEntity(event.target.checked);
  };

  const currentNodeDetailsChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    setNodeDetailsChecked(event.target.checked);
  };

  const handleChange = (newValue: any) => {
    if (!isLineageOnDemandEnabled) {
      fetchGraph({
        queryParam: { depth: parseInt(newValue) },
        legends: false
      });
    }

    if (isLineageOnDemandEnabled) {
      initialQueryObj[this.guid].depth = lineageDepth;
      fetchGraph({
        queryParam: initialQueryObj,
        legends: false
      });
    }
    setValue({ label: newValue });
  };
  const handleNodeValueChange = (newValue: any) => {
    setNodeValue(newValue);
  };

  const handleSearchChange = (
    e: { stopPropagation: () => void },
    newValue: string | null
  ) => {
    const { label, value }: any = newValue;
    e.stopPropagation();
    searchNode({ guid: value });
    setTypeValue(label);
  };

  const onCheckUnwantedEntity = (
    e: ChangeEvent<HTMLInputElement>,
    type: string
  ) => {
    if (type === "checkHideProcess") {
      filterObj.current.isProcessHideCheck = e.target.checked;
    } else {
      filterObj.current.isDeletedEntityHideCheck = e.target.checked;
    }
    refresh({
      compactLineageEnabled: isLineageOnDemandEnabled,
      filterObj: { filterObj }
    });
  };

  const isLineageOptionsEnabled = !isEmpty(lineageData?.relations)
    ? true
    : false;

  return (
    <>
      <Stack height="100%" position="relative">
        <Stack
          height="60px"
          className={`${isFullscreen ? "fullscreen" : ""}`}
          direction="row"
          maxHeight="60px"
          justifyContent="space-between"
          padding="0 1rem"
          gap="1rem"
          alignItems="flex-start"
        >
          <Stack
            className="legends pull-left"
            padding="1rem 0"
            ref={legendRef}
          ></Stack>
          <Stack padding="1rem 0" direction="row" gap="8px">
            <LightTooltip title="Realign Lineage">
              <span>
                <IconButton
                  size="small"
                  onClick={() => {
                    onClickResetLineage();
                  }}
                  disabled={!isLineageOptionsEnabled}
                >
                  <RefreshIcon />
                </IconButton>
              </span>
            </LightTooltip>
            <LightTooltip title="Export to PNG">
              <span>
                <IconButton
                  size="small"
                  onClick={exportLineage}
                  disabled={!isLineageOptionsEnabled}
                >
                  <CameraAltIcon />
                </IconButton>
              </span>
            </LightTooltip>
            <LightTooltip title="Settings">
              <span>
                <IconButton
                  size="small"
                  onClick={(e) => {
                    handleSettingsPopoverOpen(e);
                  }}
                  disabled={!isLineageOptionsEnabled}
                >
                  <SettingsIcon />
                </IconButton>
              </span>
            </LightTooltip>
            <LightTooltip title="Filter">
              <span>
                <IconButton
                  size="small"
                  onClick={(e) => {
                    handleFiltersPopoverOpen(e);
                  }}
                  disabled={!isLineageOptionsEnabled}
                >
                  <FilterIcon />
                </IconButton>
              </span>{" "}
            </LightTooltip>
            <LightTooltip title="Search">
              <span>
                <IconButton
                  size="small"
                  onClick={(e) => {
                    handleSearchPopoverOpen(e);
                  }}
                  disabled={!isLineageOptionsEnabled}
                >
                  <SearchIcon />
                </IconButton>
              </span>
            </LightTooltip>
            <Stack direction="row">
              <LightTooltip title="Zoom In">
                <span>
                  <IconButton
                    disabled={!isLineageOptionsEnabled}
                    size="small"
                    onClick={zoomIn}
                  >
                    <ZoomInIcon />
                  </IconButton>
                </span>
              </LightTooltip>
              <LightTooltip title="Zoom Out">
                <span>
                  <IconButton
                    disabled={!isLineageOptionsEnabled}
                    size="small"
                    title="Zoom Out"
                    onClick={zoomOut}
                  >
                    <ZoomOutIcon />
                  </IconButton>
                </span>
              </LightTooltip>
            </Stack>
            <LightTooltip title={isFullscreen ? "Default View" : "Full Screen"}>
              <span>
                <IconButton
                  onClick={toggleFullscreen}
                  disabled={!isLineageOptionsEnabled}
                  size="small"
                >
                  <FullscreenIcon />
                </IconButton>
              </span>
            </LightTooltip>
          </Stack>
        </Stack>
        <Stack
          direction="row"
          className={`${isFullscreen ? "fullscreen" : ""}`}
          top="60px"
        >
          {loader ? (
            <Stack
              width="100%"
              height={isFullscreen ? "calc(100vh - 60px)" : "550px"}
              justifyContent="center"
              alignItems="center"
            >
              <CircularProgress />
            </Stack>
          ) : (
            <Stack
              className="svg"
              ref={lineageDivRef}
              // height="100%"
              height={isFullscreen ? "calc(100vh - 60px)" : "550px"}
              flex="1"
              direction="row"
            ></Stack>
          )}
          <Stack
            marginTop="10px"
            style={{
              display: drawerOpen ? "block" : "none",
              position: drawerOpen ? "absolute" : "relative",
              width: drawerOpen ? "400px" : "unset",
              right: drawerOpen ? 8 : "unset",
              top: drawerOpen ? 40 : "unset",
              zIndex: drawerOpen ? 99999 : "unset",
              background: "white"
            }}
          >
            <Slide direction="left" in={drawerOpen}>
              <Stack>
                <Stack
                  direction="row"
                  justifyContent={"center"}
                  padding={"12px 16px"}
                  borderRadius="8px 8px 0 0"
                  style={{ background: "#4a90e2" }}
                >
                  <Typography
                    variant="h6"
                    color="white"
                    flex={1}
                    fontWeight="600"
                  >
                    {nodeDetails?.valueObject?.typeName}
                  </Typography>
                  <Button
                    onClick={() => setDrawerOpen(false)}
                    size="small"
                    sx={{ padding: 0, minWidth: "24px", color: "white" }}
                  >
                    <CloseIcon />
                  </Button>
                </Stack>

                <Stack
                  borderRadius="0 0 8px 8px"
                  padding={"12px 16px"}
                  border={"1px solid rgba(0,0,0,0.12)"}
                >
                  {!isEmpty(nodeDetails?.valueObject)
                    ? Object.entries(nodeDetails.valueObject)
                        .sort()
                        .map(([keys, value]: [string, any]) => {
                          // if (keys == "guid") {
                          const searchParams = new URLSearchParams(
                            location.search
                          );

                          searchParams.set("tabActive", "lineage");

                          // return (
                          <Link
                            className="entity-name text-center text-blue text-decoration-none"
                            to={{
                              pathname: `/detailPage/${nodeDetails?.valueObject[keys]}`,
                              search: `?${searchParams.toString()}`
                            }}
                            color={"primary"}
                          >
                            {nodeDetails?.valueObject[keys]}{" "}
                          </Link>;
                          // );
                          // }
                          return (
                            <>
                              <Stack
                                direction="row"
                                marginBottom={1}
                                marginTop={1}
                              >
                                <div
                                  style={{
                                    flexBasis: "200px",
                                    wordBreak: "break-all",
                                    textAlign: "left",
                                    fontWeight: "600"
                                  }}
                                >
                                  {keys}
                                </div>
                                <div
                                  style={{
                                    flex: 1,
                                    wordBreak: "break-all",
                                    textAlign: "left"
                                  }}
                                >
                                  {keys == "guid" ? (
                                    <Link
                                      className="nav-link text-blue"
                                      to={{
                                        pathname: `/detailPage/${nodeDetails?.valueObject[keys]}`,
                                        search: `?${searchParams.toString()}`
                                      }}
                                      color={"primary"}
                                    >
                                      {nodeDetails?.valueObject[keys]}{" "}
                                    </Link>
                                  ) : (
                                    getValues(
                                      nodeDetails?.valueObject[keys],
                                      undefined,
                                      entity,
                                      undefined,
                                      "properties",
                                      undefined,
                                      entity,
                                      keys
                                    )
                                  )}
                                </div>
                              </Stack>
                              <Divider />
                            </>
                          );
                        })
                    : "No Record Found"}
                </Stack>
              </Stack>
            </Slide>
          </Stack>
        </Stack>

        <Popover
          id={searchPopoverId}
          open={openSearchoPover}
          anchorEl={searchPopover}
          onClose={handleSearchPopoverClose}
          disablePortal={false}
          sx={{
            "& .MuiPopover-paper": {
              width: "250px"
            }
          }}
          anchorOrigin={{
            vertical: "bottom",
            horizontal: "center"
          }}
          transformOrigin={{
            vertical: "top",
            horizontal: "center"
          }}
        >
          <Stack>
            <Stack
              direction="row"
              justifyContent="space-between"
              alignItems="center"
              sx={{
                padding: 1.5,
                backgroundColor: "#4a90e2",
                borderBottom: "1px solid #ddd",
                borderRadius: "4px 4px 0 0"
              }}
            >
              <Typography variant="h6" color="white" fontWeight="600">
                Search
              </Typography>
              <Button
                onClick={handleSearchPopoverClose}
                size="small"
                sx={{ padding: 0, minWidth: "24px", color: "white" }}
              >
                <CloseIcon />
              </Button>
            </Stack>

            <Stack padding="1rem 0.5rem">
              <Stack gap="1rem">
                <Stack direction="row" gap="0.5rem">
                  <Typography className="menuitem-label">
                    Search Lineage Entity:
                  </Typography>
                </Stack>

                <Stack direction="row" gap="0.5rem">
                  <FormControl
                    size="medium"
                    className="advanced-search-formcontrol"
                  >
                    <Autocomplete
                      size="small"
                      value={typeValue}
                      onChange={(e: any, newValue: string | null) => {
                        handleSearchChange(e, newValue);
                      }}
                      disableClearable
                      id="select-node"
                      options={lineageSearchOptions}
                      className="advanced-search-autocomplete"
                      renderInput={(params) => (
                        <TextField
                          {...params}
                          fullWidth
                          label="Select Node"
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
                </Stack>
              </Stack>
            </Stack>
          </Stack>
        </Popover>

        <Popover
          id={settingPopoverId}
          open={openSettingsPopover}
          anchorEl={settingsPopover}
          onClose={handlePopoverClose}
          anchorOrigin={{
            vertical: "bottom",
            horizontal: "center"
          }}
          transformOrigin={{
            vertical: "top",
            horizontal: "left"
          }}
          sx={{
            "& .MuiPopover-paper": {
              width: "250px"
            }
          }}
        >
          <Stack>
            <Stack
              direction="row"
              justifyContent="space-between"
              alignItems="center"
              sx={{
                padding: 1.5,
                backgroundColor: "#4a90e2",
                borderBottom: "1px solid #ddd",
                borderRadius: "4px 4px 0 0"
              }}
            >
              <Typography variant="h6" color="white" fontWeight="600">
                Settings
              </Typography>
              <Button
                onClick={handlePopoverClose}
                size="small"
                sx={{ padding: 0, minWidth: "24px", color: "white" }}
              >
                <CloseIcon />
              </Button>
            </Stack>

            <Stack padding="1rem 0.5rem">
              <Stack gap="1rem">
                <Stack direction="row" gap="0.5rem">
                  <AntSwitch
                    sx={{ marginRight: "4px" }}
                    size="small"
                    checked={currentPathChecked}
                    onChange={(e) => {
                      currentPathHandleChange(e);
                    }}
                    inputProps={{ "aria-label": "controlled" }}
                  />
                  <Typography className="menuitem-label">
                    On hover show current path
                  </Typography>
                </Stack>
                <Stack direction="row" gap="0.5rem">
                  <AntSwitch
                    sx={{ marginRight: "4px" }}
                    size="small"
                    checked={nodeDetailsChecked}
                    onChange={(e) => {
                      currentNodeDetailsChange(e);
                    }}
                    inputProps={{ "aria-label": "controlled" }}
                  />
                  <Typography className="menuitem-label">
                    Show node details on hover
                  </Typography>
                </Stack>
                <Stack direction="row" gap="0.5rem">
                  <AntSwitch
                    sx={{ marginRight: "4px" }}
                    size="small"
                    checked={fullNamechecked}
                    onChange={(e) => {
                      fullNameHandleChange(e);
                    }}
                    inputProps={{ "aria-label": "controlled" }}
                  />
                  <Typography className="menuitem-label">
                    Display full name
                  </Typography>
                </Stack>
                {isLineageOnDemandEnabled && (
                  <Stack direction="row" gap="0.5rem">
                    Nodes on Demand:
                    <Stack className="table-pagination-filters-box">
                      <FormControl fullWidth size="small">
                        <Autocomplete
                          value={nodeValue}
                          disableClearable
                          onChange={(_event: any, newValue) => {
                            if (typeof newValue === "string") {
                              setValue({
                                label: newValue
                              });
                            } else if (newValue && newValue.inputValue) {
                              setValue({
                                label: newValue.inputValue
                              });
                            } else {
                              setValue(newValue);
                            }
                            handleNodeValueChange(newValue);
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
                          defaultValue={3}
                          selectOnFocus
                          clearOnBlur
                          handleHomeEndKeys
                          id="Nodes"
                          options={nodeCountArray.map((obj) => ({
                            label: obj
                          }))}
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
                          sx={{ width: 100 }}
                          freeSolo
                          renderInput={(params) => (
                            <TextField
                              type="number"
                              {...params}
                              label="Nodes:"
                            />
                          )}
                        />
                      </FormControl>
                    </Stack>
                  </Stack>
                )}
              </Stack>
            </Stack>
          </Stack>
        </Popover>
        <Popover
          id={filterPopoverId}
          open={openFiltersPopover}
          anchorEl={filterPopover}
          onClose={handleFilterPopoverClose}
          anchorOrigin={{
            vertical: "bottom",
            horizontal: "center"
          }}
          transformOrigin={{
            vertical: "top",
            horizontal: "left"
          }}
          sx={{
            "& .MuiPopover-paper": {
              width: "250px"
            }
          }}
        >
          <Stack>
            <Stack
              direction="row"
              justifyContent="space-between"
              alignItems="center"
              sx={{
                padding: 1.5,
                backgroundColor: "#4a90e2",
                borderBottom: "1px solid #ddd",
                borderRadius: "4px 4px 0 0"
              }}
            >
              <Typography flex="1" variant="h6" color="white" fontWeight="600">
                Filters
              </Typography>
              <Button
                onClick={handleFilterPopoverClose}
                size="small"
                sx={{ padding: 0, minWidth: "24px", color: "white" }}
              >
                <CloseIcon />
              </Button>
            </Stack>

            <Stack padding="1rem 0.5rem">
              <Stack gap="1rem">
                <Stack gap="1rem">
                  {!isProcess && (
                    <Stack direction="row" gap="0.5rem">
                      <AntSwitch
                        sizes="small"
                        sx={{ marginRight: "4px" }}
                        checked={hideProcess}
                        onChange={(e) => {
                          handleHideProcessChange(e);
                          onCheckUnwantedEntity(e, "checkHideProcess");
                        }}
                        inputProps={{ "aria-label": "controlled" }}
                      />
                      <Typography line className="menuitem-label">
                        Hide Process
                      </Typography>
                    </Stack>
                  )}
                  <Stack direction="row" gap="0.5rem">
                    <AntSwitch
                      size="small"
                      sx={{ marginRight: "4px" }}
                      checked={checkedDeletedEntity}
                      onChange={(e) => {
                        handleDeletedEntityChange(e);
                        onCheckUnwantedEntity(e, "");
                      }}
                      inputProps={{ "aria-label": "controlled" }}
                    />
                    <Typography className="menuitem-label">
                      Hide Deleted Entity
                    </Typography>
                  </Stack>
                </Stack>
                <Stack direction="row" gap="0.5rem">
                  Depth:
                  <Stack className="table-pagination-filters-box">
                    <FormControl fullWidth size="small">
                      <Autocomplete
                        value={value}
                        disableClearable
                        onChange={(_event: any, newValue) => {
                          if (typeof newValue === "string") {
                            setValue({
                              label: newValue
                            });
                          } else if (newValue && newValue.inputValue) {
                            setValue({
                              label: newValue.inputValue
                            });
                          } else {
                            setValue({ label: newValue });
                          }
                          if (isEmpty(newValue?.inputValue)) {
                            handleChange(newValue?.label);
                          } else {
                            handleChange(newValue?.inputValue);
                          }
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
                        // defaultValue={[{ label: "3" }]}
                        selectOnFocus
                        clearOnBlur
                        handleHomeEndKeys
                        options={optionsVal}
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
                        sx={{ width: 150 }}
                        freeSolo
                        renderInput={(params) => (
                          <TextField type="number" {...params} />
                        )}
                      />
                    </FormControl>
                  </Stack>
                </Stack>
              </Stack>
            </Stack>
          </Stack>
        </Popover>

        {propagationModal && (
          <PropagationPropertyModal
            propagationModal={propagationModal}
            setPropagationModal={setPropagationModal}
            propagateDetails={propagateDetails}
            lineageData={lineageData}
            fetchGraph={fetchGraph}
            initialQueryObj={initialQueryObj}
            refresh={refresh}
          />
        )}
      </Stack>
    </>
  );
};

export default LineageTab;
