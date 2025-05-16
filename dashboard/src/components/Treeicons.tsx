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

import FolderOutlinedIcon from "@mui/icons-material/FolderOutlined";
import InsertDriveFileOutlinedIcon from "@mui/icons-material/InsertDriveFileOutlined";
import SellOutlinedIcon from "@mui/icons-material/SellOutlined";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import Avatar from "@mui/material/Avatar";
export const TreeIcons = (props: {
  node: any;
  treeName: string;
  isEmptyServicetype: boolean;
}) => {
  const { node, treeName, isEmptyServicetype } = props;
  if (treeName == "Entities" || treeName == "Business MetaData") {
    return node.children !== undefined ? (
      <FolderOutlinedIcon className="custom-treeitem-icon" />
    ) : (
      <InsertDriveFileOutlinedIcon />
    );
  } else if (treeName == "Glossary") {
    return node.types == "parent" ? (
      <FolderOutlinedIcon className="custom-treeitem-icon" />
    ) : (
      <InsertDriveFileOutlinedIcon />
    );
  } else if (treeName == "Classifications") {
    return <SellOutlinedIcon className="custom-treeitem-icon" />;
  } else if (treeName == "Relationships") {
    return <LinkOutlinedIcon className="custom-treeitem-icon" />;
  } else if (treeName == "CustomFilters") {
    return node.types == "parent" && isEmptyServicetype ? (
      <FolderOutlinedIcon className="custom-treeitem-icon" />
    ) : (
      <Avatar
        sx={(theme) => ({
          background: theme.palette.success.main,
          width: 18,
          height: 18,
          fontSize: "0.7rem"
        })}
      >
        {node.parent == "BASIC_RELATIONSHIP" ? "R" : (node.parent as string)[0]}
      </Avatar>
    );
  }
};

export default TreeIcons;
