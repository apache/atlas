//@ts-nocheck

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
import { useParams } from "react-router-dom";
import { isEmpty } from "@utils/Utils";
import {
  Autocomplete,
  Button,
  Divider,
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
import { useAppSelector } from "@hooks/reducerHook";
import {
  cloneDeep,
  extend,
  omit,
  sortByKeyWithUnderscoreFirst
} from "@utils/Helper";
import { getValues } from "@components/commonComponents";
import { lineageDepth } from "@utils/Enum";
import { AntSwitch } from "@utils/Muiutils";

const TypeSystemTreeView = ({ entityDefs }: any) => {
  const optionsVal = [
    { label: "3" },
    { label: "6" },
    { label: "9" },
    { label: "12" },
    { label: "15" },
    { label: "18" },
    { label: "21" }
  ];
  const { guid } = useParams();
  let lineageDivRef = useRef(null);
  let lineageHelperRef = useRef(null);
  let selectedDetailNode = {};
  const [isFullScreen, setIsFullScreen] = useState(false);
  const typeSystemTreeViewPageRef = useRef(null);
  const panelRef = useRef(null);
  const tableBodyRef = useRef(null);

  const [lineageMethods, setLineageMethods] = useState({});
  const [settingsPopover, setSettingsPopover] = useState(null);
  const [filterPopover, setFilterPopover] = useState(null);
  const [searchPopover, setSearchPopover] = useState(null);
  const [typeValue, setTypeValue] = useState("");
  const [searchTypeValue, setSearchTypeValue] = useState("");
  const openSettingsPopover = Boolean(settingsPopover);
  const openSearchoPover = Boolean(searchPopover);
  const searchPopoverId = searchPopover ? "simple-popover" : undefined;
  const settingPopoverId = openSettingsPopover ? "simple-popover" : undefined;
  const openFiltersPopover = Boolean(filterPopover);
  const filterPopoverId = openSettingsPopover ? "simple-popover" : undefined;
  const [fullNamechecked, setFullNameChecked] = useState(false);
  const [currentPathChecked, setCurrentPathChecked] = useState(true);
  const [nodeDetailsChecked, setNodeDetailsChecked] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [nodeDetails, setNodeDetails] = useState({});
  const [value, setValue] = useState<any>();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);

  const filterObj = {
    isProcessHideCheck: false,
    isDeletedEntityHideCheck: false,
    depthCount: lineageDepth
  };
  const { isProcessHideCheck, isDeletedEntityHideCheck } = filterObj;

  const toggleFullscreen = () => {
    setIsFullscreen(!isFullscreen);
  };

  const handleSettingsPopoverOpen = (event) => {
    setSettingsPopover(event.currentTarget);
  };
  const handleFiltersPopoverOpen = (event) => {
    setFilterPopover(event.currentTarget);
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

  useEffect(() => {
    let node = lineageDivRef.current.getBoundingClientRect();

    const lineageGraph = new LineageHelper({
      el: lineageDivRef.current,
      legends: false,
      setDataManually: true,
      width: node.width,
      height: node.height,
      zoom: true,
      fitToScreen: true,
      dagreOptions: {
        rankdir: "tb"
      },
      toolTipTitle: "Type",
      getFilterObj: function () {
        return {
          isProcessHideCheck: isProcessHideCheck,
          isDeletedEntityHideCheck: isDeletedEntityHideCheck
        };
      },
      onNodeClick: function (d) {
        updateDetails(lineageHelperRef.current.getNode(d.clickedData, true));
      },
      isShowHoverPath: function () {
        return currentPathChecked;
      },
      isShowTooltip: function () {
        return nodeDetailsChecked;
      }
    });
    // LineageHelperRef = lineageGraph;
    lineageHelperRef.current = lineageGraph;
    setLineageMethods(lineageGraph);
    fetchGraph();
  }, [currentPathChecked, nodeDetailsChecked]);

  const fetchGraph = (options: any) => {
    let typeDef = !isEmpty(entityDefs) ? cloneDeep(entityDefs) : [];
    let entityTypeDef = sortByKeyWithUnderscoreFirst(typeDef, "name");
    if (entityTypeDef.length) {
      generateData(extend(true, {}, { data: entityTypeDef }, options)).then(
        function (graphObj) {
          createGraph(options);
        }
      );
    }
  };

  const createGraph = (opt) => {
    if (lineageHelperRef.current) {
      lineageHelperRef.current.createGraph();
    }
  };

  const generateData = (options) => {
    return new Promise(function (resolve, reject) {
      try {
        let styleObj = {
          fill: "none",
          stroke: "#ffb203",
          width: 3
        };
        const makeNodeData = function (relationObj) {
          if (!relationObj || !relationObj.guid || !relationObj.name)
            return undefined;
          if (relationObj.updatedValues) {
            return relationObj;
          }
          var obj = Object.assign(relationObj, {
            shape: "img",
            updatedValues: true,
            label: relationObj.name.trunc(18),
            toolTipLabel: relationObj.name,
            id: relationObj.guid,
            isLineage: true,
            isIncomplete: false
          });
          return obj;
        };
        const getStyleObjStr = function (styleObj) {
          return (
            "fill:" +
            styleObj.fill +
            ";stroke:" +
            styleObj.stroke +
            ";stroke-width:" +
            styleObj.width
          );
        };
        const setNode = function (guid, obj) {
          // Defensive: Only create node if obj is defined and has required properties
          if (!obj || !obj.name || !obj.guid) return;
          var node = lineageHelperRef?.current?.getNode(guid);
          if (!node) {
            var nodeData = makeNodeData(obj);
            if (nodeData) {
              lineageHelperRef?.current?.setNode(guid, nodeData);
              return nodeData;
            }
            return;
          } else {
            return node;
          }
        };
        const setEdge = function (fromNodeGuid, toNodeGuid) {
          if (!fromNodeGuid || !toNodeGuid) return;
          lineageHelperRef?.current?.setEdge(fromNodeGuid, toNodeGuid, {
            arrowhead: "arrowPoint",
            style: getStyleObjStr(styleObj),
            styleObj: styleObj
          });
        };
        const setGraphData = function (fromEntityId, toEntityId) {
          setNode(
            fromEntityId,
            options.data.find((obj) => obj.guid === fromEntityId)
          );
          setNode(
            toEntityId,
            options.data.find((obj) => obj.guid === toEntityId)
          );
          setEdge(fromEntityId, toEntityId);
        };

        if (options.data) {
          if (options.filter) {
            // filter
            let pendingSuperList = {};
            let outOfFilterData = {};
            let doneList = {};

            const linkParents = function (obj) {
              if (!obj || !obj.superTypes || !Array.isArray(obj.superTypes))
                return;
              if (obj.superTypes.length) {
                for (const superType of obj.superTypes) {
                  var fromEntityId = obj.guid;
                  var tempObj =
                    doneList[superType] || outOfFilterData[superType];
                  if (tempObj && tempObj.guid && tempObj.name) {
                    if (!doneList[superType]) {
                      setNode(tempObj.guid, tempObj);
                    }
                    setEdge(tempObj.guid, fromEntityId);
                    linkParents(tempObj);
                  } else {
                    if (pendingSuperList[superType]) {
                      pendingSuperList[superType].push(fromEntityId);
                    } else {
                      pendingSuperList[superType] = [fromEntityId];
                    }
                  }
                }
              }
            };

            for (const obj of options.data) {
              if (!obj || !obj.guid || !obj.name) continue;
              let fromEntityId = obj.guid;
              if (pendingSuperList[obj.name]) {
                doneList[obj.name] = obj;
                setNode(fromEntityId, obj);
                pendingSuperList[obj.name].map(() =>
                  setEdge(fromEntityId, guid)
                );
                delete pendingSuperList[obj.name];
                linkParents(obj);
              }
              if (obj.serviceType === options.filter) {
                doneList[obj.name] = obj;
                setNode(fromEntityId, obj);
                linkParents(obj);
              } else if (!doneList[obj.name] && !outOfFilterData[obj.name]) {
                outOfFilterData[obj.name] = obj;
              }
            }

            pendingSuperList = null;
            doneList = null;
            outOfFilterData = null;
          } else {
            let pendingList = {};
            let doneList = {};

            for (const obj of options.data) {
              if (!obj || !obj.guid || !obj.name) continue;
              let fromEntityId = obj.guid;
              doneList[obj.name] = obj;
              setNode(fromEntityId, obj);
              if (pendingList[obj.name]) {
                pendingList[obj.name].map((guid) =>
                  setEdge(guid, fromEntityId)
                );
                delete pendingList[obj.name];
              }
              if (obj.subTypes && obj.subTypes.length) {
                for (const subTypes of obj.subTypes) {
                  if (doneList[subTypes]) {
                    setEdge(fromEntityId, doneList[subTypes].guid);
                  } else {
                    if (pendingList[subTypes]) {
                      pendingList[subTypes].push(fromEntityId);
                    } else {
                      pendingList[subTypes] = [fromEntityId];
                    }
                  }
                }
              }
            }
            pendingList = null;
            doneList = null;
          }
        }
        resolve(this?.g);
      } catch (e) {
        reject(e);
      }
    });
  };

  const updateDetails = (data) => {
    selectedDetailNode = {};
    selectedDetailNode.atttributes = data.attributeDefs;
    selectedDetailNode.businessAttributes = data.businessAttributeDefs;
    selectedDetailNode.relationshipAttributes = data.relationshipAttributeDefs;
    //atttributes
    data["atttributes"] = (data.attributeDefs || []).map(function (obj) {
      return obj.name;
    });
    //businessAttributes
    data["businessAttributes"] = Object.keys(data.businessAttributeDefs);
    //relationshipAttributes
    data["relationshipAttributes"] = (data.relationshipAttributeDefs || []).map(
      function (obj) {
        return obj.name;
      }
    );

    setNodeDetails({
      valueObject: omit(data, [
        "id",
        "attributeDefs",
        "relationshipAttributeDefs",
        "businessAttributeDefs",
        "isLineage",
        "isIncomplete",
        "label",
        "shape",
        "toolTipLabel",
        "updatedValues"
      ])
    });
    setDrawerOpen(true);
  };
  const fullNameHandleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    event.stopPropagation();
    setFullNameChecked(event.target.checked);
    if (lineageHelperRef.current) {
      lineageHelperRef.current.displayFullName({
        bLabelFullText: event.target.checked == true ? true : false
      });
    }
  };

  const currentPathHandleChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    setCurrentPathChecked(event.target.checked);
  };
  const currentNodeDetailsChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();
    setNodeDetailsChecked(event.target.checked);
  };

  const handleChange = (newValue: any) => {
    setValue(newValue);
  };

  const onClickSaveSvg = (e, a) => {
    lineageMethods.exportLineage({ downloadFileName: "TypeSystemView" });
  };

  const onClickReset = () => {
    lineageMethods.refresh();
    fetchGraph({ refresh: true });
    setTypeValue("");
    setSearchTypeValue("");
  };

  const renderFilterSearch = () => {
    let nodes = lineageHelperRef?.current?.getNodes() || {};
    let tempFilteMap = {};

    for (let obj in nodes) {
      if (
        nodes?.[obj]?.serviceType &&
        !tempFilteMap?.[nodes[obj]?.serviceType]
      ) {
        tempFilteMap[nodes[obj].serviceType] = {
          label: nodes[obj].serviceType,
          value: nodes[obj].guid
        };
      }
    }

    let options = !isEmpty(tempFilteMap)
      ? Object.values(tempFilteMap)
          .sort()
          .map((obj) => ({ label: obj.label, value: obj.value }))
      : [];
    return options;
  };

  const renderTypeSearch = () => {
    let nodes = lineageHelperRef?.current?.getNodes() || {};

    let options = !isEmpty(nodes)
      ? Object.values(nodes)
          .sort()
          .filter(Boolean)
          .map((obj) => ({ label: obj.name, value: obj.guid }))
      : [];
    return options;
  };

  const filterData = (value) => {
    lineageMethods.refresh();
    fetchGraph({ filter: value });
  };

  const handleFilterChange = (
    e: { stopPropagation: () => void },
    newValue: string | null
  ) => {
    e.stopPropagation();
    const { label, value }: any = newValue;
    if (!isRefreshing) {
      filterData(label);
    } else {
      setIsRefreshing(false);
    }
    setTypeValue(label);
  };

  const handleSearchChange = (
    e: { stopPropagation: () => void },
    newValue: string | null
  ) => {
    e.stopPropagation();
    const { label, value }: any = newValue;

    if (!isRefreshing) {
      lineageMethods.searchNode({ guid: value });
    } else {
      setIsRefreshing(false);
    }
    setSearchTypeValue(label);
  };

  return (
    <Stack height="100%">
      <Stack
        height="60px"
        className={`${isFullscreen ? "fullscreen" : ""}`}
        direction="row"
        maxHeight="60px"
        overflow="auto"
        justifyContent="flex-end"
        padding="1rem"
      >
        <Stack direction="row" gap="8px">
          <LightTooltip title="Reset">
            <IconButton
              size="small"
              onClick={() => {
                onClickReset();
              }}
            >
              <RefreshIcon />
            </IconButton>
          </LightTooltip>
          <LightTooltip title="Export to PNG">
            <IconButton size="small" onClick={onClickSaveSvg}>
              <CameraAltIcon />
            </IconButton>
          </LightTooltip>
          <LightTooltip title="Settings">
            <IconButton
              size="small"
              onClick={(e) => {
                handleSettingsPopoverOpen(e);
              }}
            >
              <SettingsIcon />
            </IconButton>
          </LightTooltip>
          <LightTooltip title="Filter">
            <IconButton
              size="small"
              onClick={(e) => {
                handleFiltersPopoverOpen(e);
              }}
            >
              <FilterIcon />
            </IconButton>
          </LightTooltip>
          <LightTooltip title="Search">
            <IconButton
              size="small"
              onClick={(e) => {
                setSearchPopover(e.currentTarget);
              }}
            >
              <SearchIcon />
            </IconButton>
          </LightTooltip>
          <Stack direction="row">
            <LightTooltip title="Zoom In">
              <IconButton
                size="small"
                onClick={() => {
                  if (lineageMethods) {
                    lineageMethods.zoomIn();
                  }
                }}
              >
                <ZoomInIcon />
              </IconButton>
            </LightTooltip>
            <LightTooltip title="Zoom Out">
              <IconButton
                size="small"
                onClick={() => {
                  if (lineageMethods) {
                    lineageMethods.zoomOut();
                  }
                }}
              >
                <ZoomOutIcon />
              </IconButton>
            </LightTooltip>
          </Stack>
          <LightTooltip title={isFullScreen ? "Default View" : "Full Screen"}>
            <IconButton size="small" onClick={toggleFullscreen}>
              <FullscreenIcon />
            </IconButton>
          </LightTooltip>
        </Stack>
      </Stack>
      <Stack
        ref={typeSystemTreeViewPageRef}
        top="60px"
        className={`${isFullscreen ? "fullscreen" : ""}`}
      >
        <Stack
          className="svg typesystem-svg"
          ref={lineageDivRef}
          // height="100%"
          height={isFullscreen ? "calc(100vh - 60px)" : "100%"}
          width="100%"
          minHeight="650px"
          flex="1"
        ></Stack>{" "}
        <Stack
          marginTop="10px"
          style={{
            display: drawerOpen ? "block" : "none",
            position: drawerOpen ? "absolute" : "relative",
            width: drawerOpen ? "400px" : "unset",
            right: drawerOpen ? 32 : "unset",
            zIndex: drawerOpen ? 99999 : "unset",
            background: "white"
          }}
        >
          <Slide direction="left" in={drawerOpen}>
            <Stack>
              <Stack
                direction="row"
                justifyContent={"space-between"}
                padding={"12px 16px"}
                borderRadius="8px 8px 0 0"
                style={{ background: "#4a90e2" }}
              >
                <Typography
                  flex={"1"}
                  variant="h6"
                  color="white"
                  fontWeight="600"
                >
                  {nodeDetails?.valueObject?.name}
                </Typography>
                <Button
                  onClick={() => setDrawerOpen(false)}
                  size="small"
                  sx={{ padding: 0, minWidth: "24px", color: "white" }}
                >
                  <CloseIcon fontSize="small" />
                </Button>
              </Stack>

              <Stack
                borderRadius="0 0 8px 8px"
                padding={"12px 16px"}
                border={"1px solid rgba(0,0,0,0.12)"}
                maxHeight={"500px"}
                overflow="auto"
              >
                {!isEmpty(nodeDetails?.valueObject)
                  ? Object.entries(nodeDetails.valueObject)
                      .sort()
                      .map(([keys, value]: [string, any]) => {
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
                                {getValues(
                                  nodeDetails?.valueObject[keys],
                                  undefined,
                                  undefined,
                                  undefined,
                                  "properties",
                                  undefined,
                                  undefined,
                                  keys
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
              <CloseIcon fontSize="small" />
            </Button>
          </Stack>

          <Stack padding="1rem 0.5rem">
            <Stack gap="1rem">
              <Stack direction="row" gap="0.5rem">
                <FormControl className="advanced-search-formcontrol">
                  <Autocomplete
                    size="small"
                    value={searchTypeValue}
                    onChange={(e: any, newValue: string | null) => {
                      handleSearchChange(e, newValue);
                    }}
                    disableClearable
                    id="select-node"
                    options={renderTypeSearch()}
                    className="advanced-search-autocomplete"
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        size="small"
                        fullWidth
                        label="Select Type"
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
              <CloseIcon fontSize="small" />
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
            </Stack>
          </Stack>
        </Stack>
      </Popover>
      <Popover
        id={filterPopoverId}
        open={openFiltersPopover}
        anchorEl={filterPopover}
        onClose={handleFilterPopoverClose}
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
              Filters
            </Typography>
            <Button
              onClick={handleFilterPopoverClose}
              size="small"
              sx={{ padding: 0, minWidth: "24px", color: "white" }}
            >
              <CloseIcon fontSize="small" />
            </Button>
          </Stack>

          <Stack padding="1rem 0.5rem">
            <Stack gap="1rem">
              <Stack gap="1rem">
                <Stack direction="row" gap="0.5rem">
                  <FormControl className="advanced-search-formcontrol">
                    <Autocomplete
                      size="small"
                      value={typeValue}
                      onChange={(e: any, newValue: string | null) => {
                        handleFilterChange(e, newValue);
                      }}
                      disableClearable
                      id="select-node"
                      options={renderFilterSearch()}
                      className="advanced-search-autocomplete"
                      renderInput={(params) => (
                        <TextField
                          {...params}
                          fullWidth
                          label="Select ServiceType"
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
        </Stack>
      </Popover>
    </Stack>
  );
};

export default TypeSystemTreeView;
