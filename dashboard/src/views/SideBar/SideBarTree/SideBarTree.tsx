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

import * as React from "react";
import { styled, alpha } from "@mui/material/styles";
import {
  TreeItem,
  TreeItemProps,
  useTreeItemState,
  TreeItemContentProps
} from "@mui/x-tree-view/TreeItem";
import clsx from "clsx";
import { SimpleTreeView } from "@mui/x-tree-view";
import {
  AutorenewIcon,
  Switch,
  MoreVertIcon,
  Divider,
  LightTooltip,
  FileDownloadIcon,
  FormatListBulletedIcon,
  AccountTreeIcon,
  FileUploadIcon,
  IconButton,
  Menu,
  MenuItem,
  ListItemIcon,
  Typography
} from "@components/muiComponents";
import AddIcon from "@mui/icons-material/Add";
import { getBusinessMetadataImportTmpl } from "@api/apiMethods/entitiesApiMethods";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import { CircularProgress, Stack } from "@mui/material";
import { isEmpty } from "@utils/Utils";
import LaunchOutlinedIcon from "@mui/icons-material/LaunchOutlined";
import { getGlossaryImportTmpl } from "@api/apiMethods/glossaryApiMethod";
import { toast } from "react-toastify";
import { TreeNode } from "@models/treeStructureType";
import ImportDialog from "@components/ImportDialog";
import TreeIcons from "@components/Treeicons";
import { useAppSelector } from "@hooks/reducerHook";
import TreeNodeIcons from "@components/TreeNodeIcons";
import ClassificationForm from "@views/Classification/ClassificationForm";
import AddUpdateGlossaryForm from "@views/Glossary/AddUpdateGlossaryForm";
import { useEffect } from "react";
import { globalSearchFilterInitialQuery } from "@utils/Global";
import RefreshIcon from "@mui/icons-material/Refresh";
import { AntSwitch } from "@utils/Muiutils";

const CustomContentRoot = styled("div")(({ theme, ...props }) => ({
  WebkitTapHighlightColor: "#0E8173",
  "&&:hover, &&.Mui-disabled, &&.Mui-focused, &&.Mui-selected, &&.Mui-selected.Mui-focused, &&.Mui-selected:hover":
    {
      backgroundColor: "transparent"
    },
  ".MuiTreeItem-contentBar": {
    position: "absolute",
    width: "100%",
    height: 24,
    left: 0
  },
  ".MuiTreeItem-iconContainer": {
    zIndex: 9
  },
  "&:hover .MuiTreeItem-contentBar": {
    backgroundColor: theme.palette.action.hover,
    "@media (hover: none)": {
      backgroundColor: "transparent"
    }
  },
  "&.Mui-disabled .MuiTreeItem-contentBar": {
    opacity: theme.palette.action.disabledOpacity,
    backgroundColor: "transparent"
  },
  "&.Mui-focused .MuiTreeItem-contentBar": {
    backgroundColor: theme.palette.action.focus
  },
  ...((props.selectedNodeType === props.node ||
    props.selectedNodeTag === props.node ||
    props.selectedNodeRelationship === props.node ||
    props.selectedNodeBM === props.node) && {
    "&.Mui-selected .MuiTreeItem-contentBar": {
      backgroundColor: "rgba(255,255,255,0.08)",
      borderLeft: "4px solid #2ccebb"
      // color: "white"
      // borderRadius: "4px"
    }
  }),
  ...(props?.selectedNode == props?.node && {
    "&.Mui-selected .MuiTreeItem-label": {
      color: "white"
    }
  }),
  ...(props?.selectedNode == props?.node && {
    "&.Mui-selected & .MuiTreeItem-content svg": {
      color: "white"
    }
  }),
  "&.Mui-selected:hover .MuiTreeItem-contentBar": {
    backgroundColor: alpha(
      theme.palette.primary.main,
      theme.palette.action.selectedOpacity + theme.palette.action.hoverOpacity
    ),

    "@media (hover: none)": {
      backgroundColor: alpha(
        theme.palette.primary.main,
        theme.palette.action.selectedOpacity
      )
    }
  },
  "&.Mui-selected.Mui-focused .MuiTreeItem-contentBar": {
    // backgroundColor: "#0E8173"
    backgroundColor: "rgba(255,255,255,0.08)"
  }
}));

const CustomContent = React.forwardRef(function CustomContent(
  props: TreeItemContentProps,
  ref
) {
  const {
    className,
    classes,
    label,
    itemId,
    icon: iconProp,
    expansionIcon,
    displayIcon
  } = props;

  const {
    disabled,
    expanded,
    selected,
    focused,
    handleExpansion,
    handleSelection,
    preventSelection
  } = useTreeItemState(itemId);

  const icon = iconProp || expansionIcon || displayIcon;

  const handleMouseDown = (
    event: React.MouseEvent<HTMLDivElement, MouseEvent>
  ) => {
    event.stopPropagation();
    preventSelection(event);
  };

  const handleClick = (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
    event.stopPropagation();
    event.preventDefault();
    handleExpansion(event);
    handleSelection(event);
  };

  return (
    <CustomContentRoot
      className={clsx(className, classes.root, {
        "Mui-expanded": expanded,
        "Mui-selected":
          props.label.props.selectedNodeType === props.label.props.node ||
          props.label.props.selectedNodeTag === props.label.props.node ||
          props.label.props.selectedNodeRelationship ===
            props.label.props.node ||
          props.label.props.selectedNodeBM === props.label.props.node ||
          selected,
        "Mui-focused": focused,
        "Mui-disabled": disabled
      })}
      sx={{ position: "relative" }}
      // selectedNode={props.label.props.selectedNode}
      selectedNodeType={props.label.props.selectedNodeType}
      selectedNodeTag={props.label.props.selectedNodeTag}
      selectedNodeRelationship={props.label.props.selectedNodeRelationship}
      selectedNodeBM={props.label.props.selectedNodeBM}
      node={props.label.props.node}
      onClick={(e) => {
        handleClick(e);
      }}
      onMouseDown={handleMouseDown}
      ref={ref as React.Ref<HTMLDivElement>}
    >
      <div className="MuiTreeItem-contentBar" />
      <div className={classes.iconContainer}>{icon}</div>
      <Typography component="div" className={classes.label}>
        {label}
      </Typography>
    </CustomContentRoot>
  );
});

const CustomTreeItem = React.forwardRef(function CustomTreeItem(
  props: TreeItemProps,
  ref: React.Ref<HTMLLIElement>
) {
  return (
    <TreeItem
      sx={{
        "& .MuiTreeItem-label": {
          // fontWeight: "400  !important",
          fontSize: "14px  !important",
          lineHeight: "24px !important",
          color: "rgba(255,255,255,0.8)"
        },

        "& .MuiTreeItem-content svg": {
          color: "rgba(255,255,255,0.8)",
          fontSize: "14px !important"
        }
      }}
      ContentComponent={CustomContent}
      {...props}
      ref={ref}
    />
  );
});

const BarTreeView: React.FC<{
  treeData: TreeNode[];
  treeName: string;
  isEmptyServicetype?: boolean;
  setisEmptyServicetype?: (setisEmptyServicetype: boolean) => void;
  refreshData: () => void;
  isGroupView?: boolean;
  setisGroupView?: (setisGroupView: boolean) => void;
  searchTerm: string;
  sideBarOpen: boolean;
  drawerWidth: number;
  loader?: boolean;
}> = ({
  treeData,
  treeName,
  setisEmptyServicetype,
  isEmptyServicetype,
  refreshData,
  isGroupView,
  setisGroupView,
  sideBarOpen,
  searchTerm,
  drawerWidth,
  loader
}) => {
  const { savedSearchData }: any = useAppSelector(
    (state: any) => state.savedSearch
  );
  const { bmguid } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const searchParams = new URLSearchParams(location.search);
  const [expand, setExpand] = React.useState<null | HTMLElement>(null);
  const [selectedNode, setSelectedNode] = React.useState<string | null>({
    type: null,
    tag: null,
    relationship: null,
    businessMetadata: null
  });

  const [openModal, setOpenModal] = React.useState<boolean>(false);
  const toastId: any = React.useRef(null);
  const open = Boolean(expand);
  const [expandedItems, setExpandedItems] = React.useState<string[]>([]);
  const [tagModal, setTagModal] = React.useState<boolean>(false);
  const [glossaryModal, setGlossaryModal] = React.useState<boolean>(false);
  const { businessMetaData }: any = useAppSelector(
    (state: any) => state.businessMetaData
  );

  const filteredData = treeData.filter((node) => {
    return (
      node.label?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (node.children &&
        node.children.some((child) =>
          child.label?.toLowerCase().includes(searchTerm.toLowerCase())
        ))
    );
  });
  const highlightText = (text: string) => {
    if (!searchTerm) return text;

    const parts = text.split(new RegExp(`(${searchTerm})`, "gi"));
    return parts.map((part, index) =>
      part.toLowerCase() === searchTerm.toLowerCase() ? (
        <span key={index} style={{ color: "#D3D3D3", fontWeight: "800" }}>
          {part}
        </span>
      ) : (
        part
      )
    );
  };

  useEffect(() => {
    const allNodeIds = filteredData.flatMap((node) => {
      return [
        node.id,
        ...(node.children ? node.children.map((child) => child.id) : [])
      ];
    });
    setExpandedItems([...allNodeIds, ...[treeName]]);
  }, [filteredData.length]);

  useEffect(() => {
    const searchParams = new URLSearchParams(location.search);
    const nodeIdFromParamsType = searchParams.get("type");
    const nodeIdFromParamsTag = searchParams.get("tag");
    const nodeIdFromParamsRelationshipName =
      searchParams.get("relationshipName");
    const nodeIdFromBMName = location.pathname.includes(
      "/administrator/businessMetadata"
    );

    // const newSelectedNode =
    //   nodeIdFromParamsType ||
    //   nodeIdFromParamsTag ||
    //   nodeIdFromParamsRelationshipName ||
    //   nodeIdFromBMName ||
    //   null;

    // if (newSelectedNode && isEmpty(bmguid)) {
    //   setSelectedNode(newSelectedNode);
    // } else {
    //   const bmObj = !isEmpty(businessMetaData?.businessMetadataDefs)
    //     ? businessMetaData?.businessMetadataDefs?.find(
    //         (obj: EnumTypeDefData) => {
    //           if (bmguid == obj.guid) {
    //             return obj;
    //           }
    //         }
    //       )
    //     : {};
    //   const { name = "" } = bmObj || {};

    //   setSelectedNode(name);
    // }

    const bmObj = !isEmpty(businessMetaData?.businessMetadataDefs)
      ? businessMetaData?.businessMetadataDefs?.find((obj: EnumTypeDefData) => {
          if (bmguid == obj.guid) {
            return obj;
          }
        })
      : {};
    const { name = "" } = bmObj || {};

    setSelectedNode({
      type: nodeIdFromParamsType,
      tag: nodeIdFromParamsTag,
      relationship: nodeIdFromParamsRelationshipName,
      businessMetadata: nodeIdFromBMName ? name : null
    });

    if (
      !nodeIdFromParamsType &&
      !nodeIdFromParamsTag &&
      !nodeIdFromParamsRelationshipName &&
      !nodeIdFromBMName
    ) {
      setSelectedNode({
        type: null,
        tag: null,
        relationship: null,
        businessMetadata: null
      });
    }
  }, [location.search]);

  const getEmptyTypesTitle = () => {
    switch (treeName) {
      case "Entities":
        return `${isEmptyServicetype ? "Hide" : "Show"} empty service types`;

      case "Classifications":
        return `${isEmptyServicetype ? "Show" : "Hide"} unused classifications`;

      case "Glossary":
        return `Show ${isEmptyServicetype ? "Category" : "Term"}`;

      case "CustomFilters":
        return `Show ${isEmptyServicetype ? "all" : "Type"}`;

      default:
        return "";
    }
  };

  const handleExpandedItemsChange = (
    _event: React.SyntheticEvent,
    newExpandedItems: string[]
  ) => {
    setExpandedItems(newExpandedItems);
  };

  const handleOpenModal = () => {
    setOpenModal(true);
  };
  const handleCloseModal = () => {
    setOpenModal(false);
  };

  const handleClickMenu = (event: React.MouseEvent<HTMLElement>) => {
    event.stopPropagation();
    setExpand(event.currentTarget);
  };

  const handleClose = () => {
    setExpand(null);
  };

  const handleCloseTagModal = () => {
    setTagModal(false);
  };
  const handleCloseGlossaryModal = () => {
    setGlossaryModal(false);
  };

  const handleClickNode = (nodeId: string) => {
    const searchParams = new URLSearchParams(location.search);
    const isTypeMatch = searchParams.get("type") === nodeId;
    const isTagMatch = searchParams.get("tag") === nodeId;
    const isRelationshipMatch = searchParams.get("relationshipName") === nodeId;
    const isBusinessMetadataMatch = location.pathname.includes(
      "/administrator/businessMetadata"
    );

    // if (
    //   isTypeMatch ||
    //   isTagMatch ||
    //   isRelationshipMatch ||
    //   isBusinessMetadataMatch
    // ) {
    //   if (isEmpty(bmguid) && treeName == "Business MetaData") {
    //     const bmObj = !isEmpty(businessMetaData?.businessMetadataDefs)
    //       ? businessMetaData?.businessMetadataDefs?.find(
    //           (obj: EnumTypeDefData) => {
    //             if (bmguid == obj.guid) {
    //               return obj.name;
    //             }
    //           }
    //         )
    //       : {};
    //     const { name = "" } = bmObj || {};
    //     setSelectedNode(name);
    //   }
    //   setSelectedNode(nodeId);
    // }

    if (isTypeMatch) {
      setSelectedNode({
        type: nodeId
      });
    }
    if (isTagMatch) {
      setSelectedNode({
        tag: nodeId
      });

      // setSelectedNodeTag(nodeId);
    }
    if (isRelationshipMatch) {
      setSelectedNode({
        relationship: nodeId
      });
      // setSelectedNodeRelationship(nodeId);
    }
    if (isBusinessMetadataMatch) {
      setSelectedNode({
        businessMetadata: nodeId
      });
      // setSelectedNodeBM(nodeId);
    }

    // else if (isEmpty(searchParams.toString())) {
    //   setSelectedNode(nodeId);
    // }
    // else {
    //   setSelectedNode(null);
    // }
  };

  const getNodeId = (node) => {
    if (treeName == "Classifications" && node.types == "parent") {
      return node.text;
    } else if (treeName == "Classifications" && node.types == "child") {
      return `${node.id}@${node.nodeName}`;
    }
    return !isEmpty(node?.parent) ? `${node.id}@${node?.parent}` : node.id;
    // if (treeName == "Classifications") {
    //   return;
    // }
    // return !isEmpty(node?.parent) ? `${node.id}@${node?.parent}` : node?.text;
  };

  const handleNodeClick = (
    node,
    treeName,
    searchParams,
    navigate,
    isEmptyServicetype,
    savedSearchData,
    toastId
  ) => {
    globalSearchFilterInitialQuery.setQuery({});
    searchParams.delete("tabActive");

    if (treeName === "Classifications") {
      handleClickNode(node.text);
    } else {
      handleClickNode(node.id);
    }

    if (node.id === "No Records Found") {
      event.stopPropagation();
      return;
    }

    if (shouldSetSearchParams(node, treeName)) {
      setSearchParams(
        node,
        treeName,
        searchParams,
        isEmptyServicetype,
        savedSearchData
      );
      navigateToPath(
        node,
        treeName,
        searchParams,
        navigate,
        isEmptyServicetype,
        toastId
      );
    }
  };

  const shouldSetSearchParams = (node, treeName) => {
    return (
      node.children === undefined ||
      isEmpty(node.children) ||
      treeName === "Classifications" ||
      (treeName === "Glossary" && node.types === "child")
    );
  };

  const setSearchParams = (
    node,
    treeName,
    searchParams,
    isEmptyServicetype,
    savedSearchData
  ) => {
    searchParams.set(
      "searchType",
      node.parent === "ADVANCED" ? "dsl" : "basic"
    );

    switch (treeName) {
      case "Entities":
        searchParams.delete("relationshipName");
        searchParams.set("type", node.id);
        break;
      case "Classifications":
        searchParams.delete("relationshipName");
        const id = node.label.split(" (")[0];
        searchParams.set("tag", id);
        break;
      case "Glossary":
        setGlossarySearchParams(node, searchParams, isEmptyServicetype);
        break;
      case "Relationships":
      case "CustomFilters":
        setCustomFiltersSearchParams(node, searchParams, savedSearchData);
        break;
      default:
        break;
    }

    searchParams.delete("attributes");
    searchParams.delete("entityFilters");
    searchParams.delete("tagFilters");
    searchParams.delete("relationshipFilters");
    searchParams.delete("pageOffset");
  };

  const setGlossarySearchParams = (node, searchParams, isEmptyServicetype) => {
    const guid =
      !isEmptyServicetype && node.cGuid !== undefined
        ? node.cGuid
        : node.guid || "";
    searchParams.delete("relationshipName");

    if (isEmptyServicetype) {
      searchParams.set("term", `${node.id}@${node.parent}`);
    } else {
      searchParams.delete("type");
      searchParams.delete("tag");
      searchParams.set("gid", node.guid as string);
    }

    searchParams.set("gtype", `${isEmptyServicetype ? "term" : "category"}`);
    searchParams.set("viewType", `${isEmptyServicetype ? "term" : "category"}`);
    searchParams.set("guid", guid);
  };

  const setCustomFiltersSearchParams = (
    node,
    searchParams,
    savedSearchData
  ) => {
    const keys = Array.from(searchParams.keys());
    for (let i = 0; i < keys.length; i++) {
      if (keys[i] !== "searchType") {
        searchParams.delete(keys[i]);
      }
    }

    if (treeName === "CustomFilters") {
      const params = savedSearchData.find((obj) => obj.name === node.id);
      for (const key in params?.searchParameters) {
        if (shouldSetCustomFilterParam(node, key)) {
          setCustomFilterParam(searchParams, key, params.searchParameters[key]);
        }
      }
      searchParams.set("isCF", "true");
    } else {
      searchParams.set("relationshipName", node.id);
    }
  };

  const shouldSetCustomFilterParam = (node, key) => {
    return (
      node.parent === "BASIC" ||
      node.parent === "ADVANCED" ||
      (node.parent === "BASIC_RELATIONSHIP" &&
        (key === "relationshipName" || key === "limit" || key === "offset"))
    );
  };

  const setCustomFilterParam = (searchParams, key, value) => {
    if (key === "limit") {
      searchParams.set("pageLimit", value || 25);
    } else if (key === "offset") {
      searchParams.set("pageOffset", value);
    } else if (key === "typeName") {
      searchParams.set("type", value);
    } else {
      searchParams.set(key, value);
    }
  };

  const navigateToPath = (
    node,
    treeName,
    searchParams,
    navigate,
    isEmptyServicetype,
    toastId
  ) => {
    switch (treeName) {
      case "Business MetaData":
        searchParams.delete("relationshipName");
        navigate(
          { pathname: `administrator/businessMetadata/${node.guid}` },
          { replace: true }
        );
        break;
      case "Glossary":
        if (!isEmptyServicetype) {
          searchParams.delete("relationshipName");
          navigate(
            {
              pathname: `glossary/${
                node.cGuid !== undefined ? node.cGuid : node.guid
              }`,
              search: searchParams.toString()
            },
            { replace: true }
          );
        } else if (node.types === "parent") {
          toast.dismiss(toastId.current);
          toastId.current = toast.warning("Create a Term or Category");
        } else {
          searchParams.delete("relationshipName");
          navigate(
            {
              pathname: "/search/searchResult",
              search: searchParams.toString()
            },
            { replace: true }
          );
        }
        break;
      case "Relationships":
      case "CustomFilters":
        if (
          treeName == "Relationships" ||
          (treeName == "CustomFilters" && node.parent == "BASIC_RELATIONSHIP")
        ) {
          navigate(
            {
              pathname: `relationship/relationshipSearchresult`,
              search: searchParams.toString()
            },
            { replace: true }
          );
        } else {
          searchParams.delete("relationshipName");
          navigate(
            {
              pathname: "/search/searchResult",
              search: searchParams.toString()
            },
            { replace: true }
          );
        }
        break;
      default:
        searchParams.delete("relationshipName");
        navigate(
          { pathname: "/search/searchResult", search: searchParams.toString() },
          { replace: true }
        );
        break;
    }
  };

  const renderTreeItem = (node: TreeNode) =>
    node?.id && (
      <CustomTreeItem
        key={node.id}
        itemId={getNodeId(node)}
        label={
          <div
            selectedNodeType={selectedNode.type}
            selectedNodeTag={selectedNode.tag}
            selectedNodeRelationship={selectedNode.relationship}
            selectedNodeBM={selectedNode.businessMetadta}
            // selectedNode={selectedNode}
            node={node.id}
            onClick={(event: React.MouseEvent<HTMLElement>) => {
              handleNodeClick(
                node,
                treeName,
                searchParams,
                navigate,
                isEmptyServicetype,
                savedSearchData,
                toastId
              );
            }}
            className="custom-treeitem-label"
            // className={clsx("custom-treeitem-label", {
            //   "Mui-selected": selectedNode === node.id
            // })}
          >
            {node.id != "No Records Found" && (
              <TreeIcons node={node} treeName={treeName} />
            )}
            <span className="tree-item-label">{highlightText(node.label)}</span>

            {(treeName == "Entities" ||
              treeName == "Classifications" ||
              treeName == "CustomFilters" ||
              treeName == "Glossary") &&
              node.id != "No Records Found" && (
                <TreeNodeIcons
                  node={node}
                  treeName={treeName}
                  updatedData={refreshData}
                  isEmptyServicetype={isEmptyServicetype}
                />
              )}
          </div>
        }
      >
        {node.children && node.children.map((child) => renderTreeItem(child))}
      </CustomTreeItem>
    );

  const downloadFile = async () => {
    try {
      let apiResp: any = {};
      if (treeName == "Entities") {
        apiResp = await getBusinessMetadataImportTmpl({});
      } else if (treeName == "Glossary") {
        apiResp = await getGlossaryImportTmpl({});
      }
      let text: string = "";
      if (apiResp) {
        text = apiResp.data;
      }
      const blob = new Blob([text], { type: "text/plain" });

      const url = window.URL.createObjectURL(blob);

      const link = document.createElement("a");
      link.href = url;
      if (treeName == "Entities") {
        link.setAttribute("download", "template_business_metadata");
      } else if (treeName == "Glossary") {
        link.setAttribute("download", "template");
      }

      document.body.appendChild(link);

      link.click();

      document.body.removeChild(link);
    } catch (error) {}
  };

  const label = { inputProps: { "aria-label": "Switch demo" } };
  return (
    <>
      <Stack
        className="sidebar-tree-box"
        sx={{
          ...(sideBarOpen == false && {
            visibility: "hidden !important",
            top: "62px !important"
          }),
          minWidth: "30px",
          width: `100% !important`,
          overflowX: "auto"
        }}
      >
        <SimpleTreeView
          expandedItems={expandedItems}
          onExpandedItemsChange={handleExpandedItemsChange}
          aria-label="customized"
          className="sidebar-treeview"
        >
          <TreeItem
            itemId={treeName}
            label={
              <Stack
                display="flex"
                alignItems="center"
                flexDirection="row"
                className="tree-item-parent-label"
              >
                <Stack flexGrow={1}>
                  <span>{treeName}</span>
                </Stack>
                <Stack direction="row" alignItems="center" gap="0.375rem">
                  <LightTooltip title="Refresh">
                    <RefreshIcon
                      onClick={(e) => {
                        e.stopPropagation();
                        refreshData();
                      }}
                      sx={{ marginRight: "5px" }}
                      fontSize="small"
                      data-cy="refreshTree"
                    />
                  </LightTooltip>

                  {treeName == "CustomFilters" && (
                    <LightTooltip title={getEmptyTypesTitle()}>
                      <AccountTreeIcon
                        sx={{
                          color: !isEmptyServicetype ? "#999 !important" : ""
                        }}
                        onClick={(e) => {
                          e.stopPropagation();
                          if (setisEmptyServicetype) {
                            setisEmptyServicetype(!isEmptyServicetype);
                          }
                        }}
                        fontSize="small"
                        className="menuitem-icon"
                      />
                    </LightTooltip>
                  )}

                  {(treeName == "Entities" ||
                    treeName == "Classifications" ||
                    treeName == "Glossary") && (
                    <>
                      {
                        <LightTooltip title={getEmptyTypesTitle()}>
                          <AntSwitch
                            {...label}
                            size="small"
                            onClick={(e: { stopPropagation: () => void }) => {
                              e.stopPropagation();
                              if (setisEmptyServicetype) {
                                setisEmptyServicetype(!isEmptyServicetype);
                              }
                            }}
                            data-cy="showEmptyServiceType"
                            inputProps={{ "aria-label": "ant design" }}
                          />
                        </LightTooltip>
                      }
                    </>
                  )}

                  {(treeName == "Entities" ||
                    treeName == "Classifications" ||
                    treeName == "Glossary") && (
                    <MoreVertIcon
                      onClick={(e: any) => {
                        e.stopPropagation();
                        handleClickMenu(e);
                      }}
                      data-cy="dropdownMenuButton"
                      fontSize="small"
                    />
                  )}

                  {treeName == "Business MetaData" && (
                    <LightTooltip title="Open Businesss Metadata">
                      <LaunchOutlinedIcon
                        fontSize="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          const newSearchParams = new URLSearchParams();

                          newSearchParams.set("tabActive", "businessMetadata");
                          navigate(
                            {
                              pathname: `administrator`,
                              search: newSearchParams.toString()
                            },
                            { replace: true }
                          );
                        }}
                        data-cy="createBusinessMetadata"
                      />
                    </LightTooltip>
                  )}
                </Stack>
                <Menu
                  onClick={(e) => {
                    e.stopPropagation();
                  }}
                  anchorEl={expand}
                  id="account-menu"
                  open={open}
                  onClose={handleClose}
                  transformOrigin={{ horizontal: "right", vertical: "top" }}
                  anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
                  sx={{
                    "& .MuiPaper-root": {
                      transition: "none !important"
                    }
                  }}
                  disableScrollLock={true}
                >
                  {(treeName == "Entities" ||
                    treeName == "Classifications") && (
                    <MenuItem
                      onClick={(e) => {
                        e.stopPropagation();
                        if (setisGroupView) {
                          setisGroupView(!isGroupView);
                        }
                      }}
                      data-cy="groupOrFlatTreeView"
                      sx={{ padding: "4px 10px" }}
                    >
                      <ListItemIcon
                        sx={{ minWidth: "28px !important" }}
                        data-cy="groupOrFlatTreeView"
                      >
                        {isGroupView ? (
                          <FormatListBulletedIcon
                            fontSize="small"
                            className="menuitem-icon"
                          />
                        ) : (
                          <AccountTreeIcon
                            fontSize="small"
                            className="menuitem-icon"
                          />
                        )}
                      </ListItemIcon>
                      <Typography className="menuitem-label">
                        Show {isGroupView ? "flat" : "group"} tree
                      </Typography>
                    </MenuItem>
                  )}
                  {(treeName == "Classifications" ||
                    treeName == "Glossary") && (
                    <MenuItem
                      onClick={(e) => {
                        e.stopPropagation();
                        if (treeName == "Classifications") {
                          setTagModal(true);
                        } else if (treeName == "Glossary") {
                          setGlossaryModal(true);
                        }
                      }}
                      data-cy="createClassification"
                      sx={{ padding: "4px 10px" }}
                    >
                      <ListItemIcon
                        sx={{ minWidth: "28px !important" }}
                        data-cy="createTag"
                      >
                        <AddIcon fontSize="small" className="menuitem-icon" />
                      </ListItemIcon>
                      <Typography className="menuitem-label">
                        Create{" "}
                        {treeName == "Classifications"
                          ? "Classifications"
                          : "Glossary"}
                      </Typography>
                    </MenuItem>
                  )}
                  {(treeName == "Entities" || treeName == "Glossary") && (
                    <MenuItem
                      onClick={(e) => {
                        e.stopPropagation();
                        downloadFile();
                      }}
                      data-cy="downloadBusinessMetadata"
                      disabled={
                        treeName == "Glossary" && !isEmptyServicetype
                          ? true
                          : false
                      }
                      sx={{ padding: "4px 10px" }}
                    >
                      <ListItemIcon sx={{ minWidth: "28px !important" }}>
                        <FileDownloadIcon
                          fontSize="small"
                          className="menuitem-icon"
                        />
                      </ListItemIcon>
                      <Typography className="menuitem-label">
                        Download Import template
                      </Typography>
                    </MenuItem>
                  )}
                  {(treeName == "Entities" || treeName == "Glossary") && (
                    <MenuItem
                      onClick={(e) => {
                        e.stopPropagation();
                        handleOpenModal();
                      }}
                      data-cy="importBusinessMetadata"
                      disabled={
                        treeName == "Glossary" && !isEmptyServicetype
                          ? true
                          : false
                      }
                      sx={{ padding: "4px 10px" }}
                    >
                      <ListItemIcon sx={{ minWidth: "28px !important" }}>
                        <FileUploadIcon
                          fontSize="small"
                          className="menuitem-icon"
                        />
                      </ListItemIcon>

                      <Typography className="menuitem-label">
                        {treeName == "Entities"
                          ? "Import Business Metadata"
                          : "Import Glossary Term"}
                      </Typography>
                    </MenuItem>
                  )}
                </Menu>
              </Stack>
            }
            sx={{
              "& .MuiTreeItem-label": {
                // textTransform: "uppercase",
                fontWeight: "600  !important",
                fontSize: "14px  !important",
                lineHeight: "26px !important",
                color: "white"
              },
              "& .MuiTreeItem-content svg": {
                color: "white",
                fontSize: "20px !important"
              }
            }}
          >
            {loader ? (
              <Stack className="tree-item-loader-box">
                <CircularProgress size="small" className="tree-item-loader" />
              </Stack>
            ) : (
              filteredData.map((node: TreeNode) => renderTreeItem(node))
            )}
          </TreeItem>

          <ImportDialog
            open={openModal}
            onClose={handleCloseModal}
            title={
              treeName == "Entities"
                ? "Import Business Metadata"
                : "Import Glossary Term"
            }
          />
        </SimpleTreeView>
      </Stack>

      {tagModal && (
        <ClassificationForm
          open={tagModal}
          isAdd={true}
          onClose={handleCloseTagModal}
        />
      )}
      {glossaryModal && (
        <AddUpdateGlossaryForm
          open={glossaryModal}
          isAdd={true}
          onClose={handleCloseGlossaryModal}
          node={undefined}
        />
      )}
    </>
  );
};

export default BarTreeView;
