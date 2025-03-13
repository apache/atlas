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
import * as React from "react";
import {
  TreeItem,
  TreeItemProps,
  useTreeItemState,
  TreeItemContentProps
} from "@mui/x-tree-view/TreeItem";
import clsx from "clsx";
import { SimpleTreeView } from "@mui/x-tree-view";
import { Typography } from "@components/muiComponents";
import { CircularProgress, Stack } from "@mui/material";
import { TreeNode } from "@models/treeStructureType";
import FolderOutlinedIcon from "@mui/icons-material/FolderOutlined";
import InsertDriveFileOutlinedIcon from "@mui/icons-material/InsertDriveFileOutlined";
import { isEmpty } from "@utils/Utils";
import { toast } from "react-toastify";

const CustomContentRoot = styled("div")(({ theme }) => ({
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
  "&.Mui-selected .MuiTreeItem-contentBar": {
    backgroundColor: "#beebff"
  },
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
    backgroundColor: "#beebff"
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
        "Mui-selected": selected,
        "Mui-focused": focused,
        "Mui-disabled": disabled
      })}
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
          fontWeight: "400  !important",
          fontSize: "1rem  !important",
          lineHeight: "24px !important",
          color: "rgba(0,0,0,0.8)"
        },

        "& .MuiTreeItem-content svg": {
          color: "rgba(0,0,0,0.8)",
          fontSize: "18px !important"
        }
      }}
      ContentComponent={CustomContent}
      {...props}
      ref={ref}
    />
  );
});

const FormTreeView: React.FC<{
  treeData: TreeNode[];
  treeName: string;
  loader?: boolean;
  onNodeSelect?: any;
  searchTerm: string;
}> = ({ treeData, treeName, loader, onNodeSelect, searchTerm }) => {
  const [expandedItems, setExpandedItems] = React.useState<string[]>([]);
  const toastId: any = React.useRef(null);

  React.useEffect(() => {
    const handleExpandClick = () => {
      setExpandedItems(() => [treeName]);
    };
    handleExpandClick();
  }, []);

  const handleExpandedItemsChange = (
    _event: React.SyntheticEvent,
    itemIds: string[]
  ) => {
    setExpandedItems(itemIds);
  };

  const filteredData = treeData.filter((node) => {
    return (
      node.label.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (node.children &&
        node.children.some((child) =>
          child.label.toLowerCase().includes(searchTerm.toLowerCase())
        ))
    );
  });

  const highlightText = (text: string) => {
    if (!searchTerm) return text;

    const parts = text.split(new RegExp(`(${searchTerm})`, "gi"));
    return parts.map((part, index) =>
      part.toLowerCase() === searchTerm.toLowerCase() ? (
        <span key={index} style={{ color: "red", fontWeight: "bold" }}>
          {part}
        </span>
      ) : (
        part
      )
    );
  };
  const renderTreeItem = (node: TreeNode) =>
    node?.id && (
      <CustomTreeItem
        key={node.id}
        itemId={node.types != "parent" ? `${node.id}@${node?.parent}` : node.id}
        label={
          <div
            onClick={(_event: React.MouseEvent<HTMLElement>) => {
              if (node.types == "parent" && isEmpty(node.children)) {
                toast.dismiss(toastId.current);
                toastId.current = toast.warning(`No ${treeName}`);
                return;
              }
            }}
            className="custom-treeitem-label"
          >
            {node.id != "No Records Found" && node.types == "parent" ? (
              <FolderOutlinedIcon className="custom-treeitem-icon" />
            ) : (
              <InsertDriveFileOutlinedIcon />
            )}
            <span className="tree-item-label">{highlightText(node.label)}</span>
          </div>
        }
      >
        {node.children && node.children.map((child) => renderTreeItem(child))}
      </CustomTreeItem>
    );

  return (
    <>
      <Stack className="sidebar-tree-box">
        <SimpleTreeView
          expandedItems={expandedItems}
          onExpandedItemsChange={handleExpandedItemsChange}
          aria-label="customized"
          defaultExpandedItems={[treeName]}
          className="sidebar-treeview"
          onItemFocus={(_e, itemId) => {
            if (itemId.indexOf("@") == -1) {
              return;
            }
            onNodeSelect(itemId);
          }}
        >
          <TreeItem
            itemId={treeName}
            label={
              <Stack
                display="flex"
                alignItems="center"
                flexDirection="row"
                sx={{ borderBottom: "1px solid rgba(25,255,255,0.1)" }}
                className="tree-item-parent-label"
              >
                <Stack flexGrow={1}>
                  <span>{treeName}</span>
                </Stack>
              </Stack>
            }
            sx={{
              "& .MuiTreeItem-label": {
                fontWeight: "bold  !important",
                fontSize: "1rem  !important",
                lineHeight: "24px !important",
                color: "#38bb9b"
              },
              "& .MuiTreeItem-content svg": {
                color: "#38bb9b",
                fontSize: "20px !important"
              }
            }}
          >
            {loader ? (
              <Stack className="tree-item-loader-box">
                <CircularProgress className="tree-item-loader" />
              </Stack>
            ) : (
              filteredData.map((node: TreeNode) => renderTreeItem(node))
            )}
          </TreeItem>
        </SimpleTreeView>
      </Stack>
    </>
  );
};

export default FormTreeView;
