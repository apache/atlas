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

import { styled, alpha } from "@mui/material/styles";
import {
  HTMLAttributes,
  isValidElement,
  useEffect,
  MouseEvent,
  forwardRef,
  Ref,
  FC,
  useRef,
  useState,
  useMemo,
  SyntheticEvent,
  memo,
} from "react";
import {
  TreeItem,
  TreeItemProps,
  useTreeItemState,
  TreeItemContentProps,
} from "@mui/x-tree-view/TreeItem";
import clsx from "clsx";
import { SimpleTreeView } from "@mui/x-tree-view";
import {
  MoreVertIcon,
  LightTooltip,
  FileDownloadIcon,
  FormatListBulletedIcon,
  AccountTreeIcon,
  FileUploadIcon,
  Menu,
  MenuItem,
  ListItemIcon,
  Typography,
} from "@components/muiComponents";
import AddIcon from "@mui/icons-material/Add";
import { getBusinessMetadataImportTmpl } from "@api/apiMethods/entitiesApiMethods";
import {
  NavigateFunction,
  useLocation,
  useNavigate,
  useParams,
} from "react-router-dom";

import Stack from "@mui/material/Stack";
import { globalSearchFilterInitialQuery, isEmpty } from "@utils/Utils";
import LaunchOutlinedIcon from "@mui/icons-material/LaunchOutlined";
import { getGlossaryImportTmpl } from "@api/apiMethods/glossaryApiMethod";
import { toast } from "react-toastify";
import { EnumTypeDefData, TreeNode } from "@models/treeStructureType";
import ImportDialog from "@components/ImportDialog";
import TreeIcons from "@components/Treeicons";
import { useAppSelector } from "@hooks/reducerHook";
import TreeNodeIcons from "@components/TreeNodeIcons";
import ClassificationForm from "@views/Classification/ClassificationForm";
import AddUpdateGlossaryForm from "@views/Glossary/AddUpdateGlossaryForm";
import RefreshIcon from "@mui/icons-material/Refresh";
import { AntSwitch } from "@utils/Muiutils";
import { IconButton } from "@components/muiComponents";
import SkeletonLoader from "@components/SkeletonLoader";

type CustomContentRootProps = HTMLAttributes<HTMLDivElement> & {
  selectedNodeType?: any;
  selectedNodeTag?: any;
  selectedNodeRelationship?: any;
  selectedNodeBM?: any;
  node?: any;
  selectedNode?: any;
};

const CustomContentRoot = styled("div")<CustomContentRootProps>(
  ({ theme, ...props }) => ({
    WebkitTapHighlightColor: "#0E8173",
    "&&:hover, &&.Mui-disabled, &&.Mui-focused, &&.Mui-selected, &&.Mui-selected.Mui-focused, &&.Mui-selected:hover":
      {
        backgroundColor: "transparent",
      },
    ".MuiTreeItem-contentBar": {
      position: "absolute",
      width: "100%",
      height: 29,
      left: 0,
    },
    ".MuiTreeItem-iconContainer": {
      zIndex: 9,
    },
    "&:hover .MuiTreeItem-contentBar": {
      backgroundColor: theme.palette.action.hover,
      "@media (hover: none)": {
        backgroundColor: "transparent",
      },
    },
    "&.Mui-disabled .MuiTreeItem-contentBar": {
      opacity: theme.palette.action.disabledOpacity,
      backgroundColor: "transparent",
    },
    "&.Mui-focused .MuiTreeItem-contentBar": {
      backgroundColor: theme.palette.action.focus,
    },
    ...((props.selectedNodeType === props.node ||
      props.selectedNodeTag === props.node ||
      props.selectedNodeRelationship === props.node ||
      props.selectedNodeBM === props.node) && {
      "&.Mui-selected .MuiTreeItem-contentBar": {
        backgroundColor: "rgba(255,255,255,0.08)",
        borderLeft: "4px solid #2ccebb",
        // color: "white"
        // borderRadius: "4px"
      },
    }),
    ...(props?.selectedNode == props?.node && {
      "&.Mui-selected .MuiTreeItem-label": {
        color: "white",
      },
    }),
    ...(props?.selectedNode == props?.node && {
      "&.Mui-selected & .MuiTreeItem-content svg": {
        color: "white",
      },
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
        ),
      },
    },
    "&.Mui-selected.Mui-focused .MuiTreeItem-contentBar": {
      // backgroundColor: "#0E8173"
      backgroundColor: "rgba(255,255,255,0.08)",
    },
  })
);

const CustomContent = forwardRef(function CustomContent(
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
    displayIcon,
  } = props;

  const {
    disabled,
    expanded,
    selected,
    focused,
    handleExpansion,
    handleSelection,
    preventSelection,
  } = useTreeItemState(itemId);

  const icon = iconProp || expansionIcon || displayIcon;

  const handleMouseDown = (event: MouseEvent<HTMLDivElement>) => {
    event.stopPropagation();
    preventSelection(event);
  };

  const handleClick = (event: MouseEvent<HTMLDivElement>) => {
    event.stopPropagation();
    event.preventDefault();
    handleExpansion(event);
    handleSelection(event);
  };

  const labelProps = isValidElement(props.label)
  ? (props.label.props as CustomContentRootProps)
  : undefined;

  return (
    <CustomContentRoot
      className={clsx(className, classes.root, {
        "Mui-expanded": expanded,
        "Mui-selected":
          (isValidElement(props.label) &&
            (labelProps?.selectedNodeType === labelProps?.node ||
              labelProps?.selectedNodeTag === labelProps?.node ||
              labelProps?.selectedNodeRelationship ===
                labelProps?.node ||
              labelProps?.selectedNodeBM === labelProps?.node)) ||
          selected,
        "Mui-focused": focused,
        "Mui-disabled": disabled,
      })}
      sx={{ position: "relative" }}
      // selectedNode={props.label.props.selectedNode}
      selectedNodeType={labelProps?.selectedNodeType}
      selectedNodeTag={labelProps?.selectedNodeTag}
      selectedNodeRelationship={labelProps?.selectedNodeRelationship}
      selectedNodeBM={labelProps?.selectedNodeBM}
      node={labelProps?.node}
      onClick={(e) => {
        handleClick(e);
      }}
      onMouseDown={handleMouseDown}
      ref={ref as Ref<HTMLDivElement>}
    >
      <div className="MuiTreeItem-contentBar" />
      <div className={classes.iconContainer}>{icon}</div>
      <Typography component="div" className={classes.label}>
        {label}
      </Typography>
    </CustomContentRoot>
  );
});

const CustomTreeItem = memo(
  forwardRef(function CustomTreeItem(
    props: TreeItemProps,
    ref: Ref<HTMLLIElement>
  ) {
    return (
      <TreeItem
        sx={{
          "& .MuiTreeItem-label": {
            // fontWeight: "400  !important",
            fontSize: "14px  !important",
            lineHeight: "24px !important",
            color: "rgba(255,255,255,0.8)",
          },

          "& .MuiTreeItem-content svg": {
            color: "rgba(255,255,255,0.8)",
            fontSize: "14px !important",
          },
        }}
        ContentComponent={CustomContent}
        {...props}
        ref={ref}
      />
    );
  })
);

const BarTreeView: FC<{
  treeData: TreeNode[];
  treeName: string;
  isEmptyServicetype?: boolean;
  setisEmptyServicetype?: (setisEmptyServicetype: boolean) => void;
  refreshData: () => void;
  isGroupView?: boolean;
  setisGroupView?: (setisGroupView: boolean) => void;
  searchTerm: string;
  sideBarOpen: boolean;
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
  loader,
}) => {
  const { savedSearchData }: any = useAppSelector(
    (state: any) => state.savedSearch
  );
  const { bmguid } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const searchParams = new URLSearchParams(location.search);
  const [expand, setExpand] = useState<null | HTMLElement>(null);
  const [selectedNode, setSelectedNode] = useState<{
    type: string | null;
    tag: string | null;
    relationship: string | null;
    businessMetadata: string | null;
  }>({
    type: null,
    tag: null,
    relationship: null,
    businessMetadata: null,
  });

  const [openModal, setOpenModal] = useState<boolean>(false);
  const toastId: any = useRef(null);
  const open = Boolean(expand);
  const [expandedItems, setExpandedItems] = useState<string[]>([]);
  const [tagModal, setTagModal] = useState<boolean>(false);
  const [glossaryModal, setGlossaryModal] = useState<boolean>(false);
  const { businessMetaData }: any = useAppSelector(
    (state: any) => state.businessMetaData
  );

  const filteredData = useMemo(() => {
    return treeData.filter((node) => {
      return (
        node.label?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        (node.children &&
          node.children.some((child) =>
            child.label?.toLowerCase().includes(searchTerm.toLowerCase())
          ))
      );
    });
  }, [treeData, searchTerm]);

  const displayTreeName = useMemo(() => {
    return treeName === "CustomFilters" ? "Custom Filters" : treeName
  }, [treeName]);

  const highlightText = useMemo(() => {
    return (text: string) => {
      if (!searchTerm) return text;

      const parts = text.split(new RegExp(`(${searchTerm})`, "gi"));
      return parts.map((part, index) =>
        part.toLowerCase() === searchTerm.toLowerCase() ? (
          <span key={index} style={{ color: "#D3D3D3", fontWeight: "600" }}>
            {part}
          </span>
        ) : (
          part
        )
      );
    };
  }, [searchTerm]);

  const expandedItemsMemo = useMemo(() => {
    const allNodeIds = filteredData.flatMap((node) => {
      return [
        node.id,
        ...(node.children ? node.children.map((child) => child.id) : []),
      ];
    });
    return [...allNodeIds, ...[treeName]];
  }, [filteredData, treeName]);

  useEffect(() => {
    setExpandedItems(expandedItemsMemo);
  }, [expandedItemsMemo]);

  useEffect(() => {
    const searchParams = new URLSearchParams(location.search);
    const nodeIdFromParamsType = searchParams.get("type");
    const nodeIdFromParamsTag = searchParams.get("tag");
    const nodeIdFromParamsRelationshipName =
      searchParams.get("relationshipName");
    const nodeIdFromBMName = location.pathname.includes(
      "/administrator/businessMetadata"
    );

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
      businessMetadata: nodeIdFromBMName ? name : null,
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
        businessMetadata: null,
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
    _event: SyntheticEvent,
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

  const handleClickMenu = (event: MouseEvent<HTMLElement>) => {
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

    if (isTypeMatch) {
      setSelectedNode({
        type: nodeId,
        tag: null,
        relationship: null,
        businessMetadata: null,
      });
    }
    if (isTagMatch) {
      setSelectedNode({
        type: null,
        tag: nodeId,
        relationship: null,
        businessMetadata: null,
      });
    }
    if (isRelationshipMatch) {
      setSelectedNode({
        type: null,
        tag: null,
        relationship: nodeId,
        businessMetadata: null,
      });
    }
    if (isBusinessMetadataMatch) {
      setSelectedNode({
        type: null,
        tag: null,
        relationship: null,
        businessMetadata: nodeId,
      });
    }
  };

  const getNodeId = (node: TreeNode) => {
    if (treeName == "Classifications" && node.types == "parent") {
      return node.label;
    } else if (treeName == "Classifications" && node.types == "child") {
      return `${node.id}@${node.label}`;
    }
    return !isEmpty(node?.parent) ? `${node.id}@${node?.parent}` : node.id;
  };

  const handleNodeClick = (
    node: TreeNode,
    treeName: string,
    searchParams: URLSearchParams,
    navigate: NavigateFunction,
    isEmptyServicetype: boolean | undefined,
    savedSearchData: any,
    toastId: any
  ) => {
    globalSearchFilterInitialQuery.setQuery({});
    searchParams.delete("tabActive");

    if (treeName === "Classifications") {
      handleClickNode(node.id);
    } else {
      handleClickNode(node.id);
    }

    if (node.id === "No Records Found") {
      if (typeof event !== "undefined" && event.stopPropagation) {
        event.stopPropagation();
      }
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

  const shouldSetSearchParams = (node: TreeNode, treeName: string) => {
    if (treeName === "CustomFilters" && node.types === "parent") {
      return false;
    }
    return (
      node.children === undefined ||
      isEmpty(node.children) ||
      treeName === "Classifications" ||
      (treeName === "Glossary" && node.types === "child")
    );
  };

  const setSearchParams = (
    node: TreeNode,
    treeName: string,
    searchParams: URLSearchParams,
    isEmptyServicetype: boolean | undefined,
    savedSearchData: any
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
    // Always reset pagination defaults on tree navigation
    if (treeName !== "CustomFilters") {
      searchParams.set("pageLimit", "25");
      searchParams.set("pageOffset", "0");
    }
  };

  const setGlossarySearchParams = (
    node: TreeNode,
    searchParams: URLSearchParams,
    isEmptyServicetype: boolean | undefined
  ) => {
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
    node: TreeNode,
    searchParams: URLSearchParams,
    savedSearchData: any[]
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

  const shouldSetCustomFilterParam = (node: TreeNode, key: string) => {
    return (
      node.parent === "BASIC" ||
      node.parent === "ADVANCED" ||
      (node.parent === "BASIC_RELATIONSHIP" &&
        (key === "relationshipName" || key === "limit" || key === "offset"))
    );
  };

  const setCustomFilterParam = (
    searchParams: URLSearchParams,
    key: string,
    value: any
  ) => {
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
    node: TreeNode,
    treeName: string,
    searchParams: URLSearchParams,
    navigate: NavigateFunction,
    isEmptyServicetype: boolean | undefined,
    toastId: any
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
              search: searchParams.toString(),
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
              search: searchParams.toString(),
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
              search: searchParams.toString(),
            },
            { replace: true }
          );
        } else {
          searchParams.delete("relationshipName");
          navigate(
            {
              pathname: "/search/searchResult",
              search: searchParams.toString(),
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

  const TreeLabelWithTooltip: React.FC<{ label: string }> = ({ label }) => {
    const labelRef = useRef<HTMLSpanElement>(null);
    const [isOverflown, setIsOverflown] = useState(false);

    useEffect(() => {
      const el = labelRef.current;
      if (el) {
        setIsOverflown(el.scrollWidth > el.clientWidth);
      }
    }, [label, searchTerm]);

    return (
      <LightTooltip title={label} disableHoverListener={!isOverflown}>
        <span
          ref={labelRef}
          className="tree-item-label"
          style={{
            display: "inline-block",
            maxWidth: "100%",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {highlightText(label)}
        </span>
      </LightTooltip>
    );
  };

  const renderTreeItem = (node: TreeNode) =>
    node?.id && (
      <CustomTreeItem
        key={node.id}
        itemId={getNodeId(node)}
        label={
          <div
            {...({
              selectedNodeType: selectedNode.type,
              selectedNodeTag: selectedNode.tag,
              selectedNodeRelationship: selectedNode.relationship,
              selectedNodeBM: selectedNode.businessMetadata,
              node: node.id,
              onClick: (_event: MouseEvent<HTMLElement>) => {
                handleNodeClick(
                  node,
                  treeName,
                  searchParams,
                  navigate,
                  isEmptyServicetype,
                  savedSearchData,
                  toastId
                );
              },
              className: "custom-treeitem-label",
            } as any)}
          >
            {node.id != "No Records Found" && (
              <TreeIcons
                node={node}
                treeName={treeName}
                isEmptyServicetype={isEmptyServicetype ?? false}
              />
            )}
            <TreeLabelWithTooltip label={node.label} />
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
            top: "62px !important",
          }),
          minWidth: "30px",
          width: `100% !important`,
          overflowX: "auto",
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
                  <span>{displayTreeName}</span>
                </Stack>
                <Stack direction="row" alignItems="center" gap="0.375rem">
                  <LightTooltip title="Refresh">
                    <IconButton
                      size="small"
                      data-cy="refreshTree"
                      onClick={(e) => {
                        e.stopPropagation();
                        refreshData();
                      }}
                      disabled={loader}
                    >
                      <RefreshIcon />
                    </IconButton>
                  </LightTooltip>

                  {treeName == "CustomFilters" && (
                    <LightTooltip title={getEmptyTypesTitle()}>
                      <AccountTreeIcon
                        sx={{
                          color: !isEmptyServicetype ? "#999 !important" : "",
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
                              search: newSearchParams.toString(),
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
                      transition: "none !important",
                    },
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
                        handleClose();
                      }}
                      data-cy="groupOrFlatTreeView"
                      className="sidebar-menu-item"
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
                        handleClose();
                      }}
                      data-cy="createClassification"
                      className="sidebar-menu-item"
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
                        handleClose();
                      }}
                      data-cy="downloadBusinessMetadata"
                      disabled={
                        treeName == "Glossary" && !isEmptyServicetype
                          ? true
                          : false
                      }
                      className="sidebar-menu-item"
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
                        handleClose();
                      }}
                      data-cy="importBusinessMetadata"
                      disabled={
                        treeName == "Glossary" && !isEmptyServicetype
                          ? true
                          : false
                      }
                      className="sidebar-menu-item"
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
                fontWeight: "600  !important",
                fontSize: "14px  !important",
                lineHeight: "26px !important",
                color: "white",
              },
              "& .MuiTreeItem-content svg": {
                color: "white",
                fontSize: "20px !important",
              },
            }}
          >
            {loader ? (
              <SkeletonLoader animation="pulse" variant="text" width={300} count={5}/>
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
