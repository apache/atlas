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

import { useRef, useState } from "react";
import IconButton from "@mui/material/IconButton";
import Menu from "@mui/material/Menu";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import MenuItem from "@mui/material/MenuItem";
import ListItemIcon from "@mui/material/ListItemIcon";
import { useNavigate } from "react-router-dom";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import CustomModal from "./Modal";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";
import { useAppSelector } from "../hooks/reducerHook";
import {
  editSavedSearch,
  removeSavedSearch
} from "../api/apiMethods/savedSearchApiMethod";
import { toast } from "react-toastify";
import { isEmpty, serverError } from "../utils/Utils";
import SearchOutlinedIcon from "@mui/icons-material/SearchOutlined";
import AddIcon from "@mui/icons-material/Add";
import ListAltOutlinedIcon from "@mui/icons-material/ListAltOutlined";
import { CustomFiltersNodeType } from "../models/customFiltersType";
import ListAltIcon from "@mui/icons-material/ListAlt";
import { addOnClassification } from "../utils/Enum";
import DeleteTag from "@views/Classification/DeleteTag";
import ClassificationForm from "@views/Classification/ClassificationForm";
import AddUpdateTermForm from "@views/Glossary/AddUpdateTermForm";
import AddUpdateGlossaryForm from "@views/Glossary/AddUpdateGlossaryForm";
import DeleteGlossary from "@views/Glossary/DeleteGlossary";
import AddUpdateCategoryForm from "@views/Glossary/AddUpdateCategoryForm";
import MoreHorizOutlinedIcon from "@mui/icons-material/MoreHorizOutlined";

const TreeNodeIcons = (props: {
  node: any;
  treeName: string;
  updatedData: any;
  isEmptyServicetype: boolean | undefined;
}) => {
  const { node, treeName, updatedData, isEmptyServicetype } = props;
  const navigate = useNavigate();
  const toastId: any = useRef(null);
  const [expandNode, setExpandNode] = useState<null | HTMLElement>(null);
  const [renameModal, setRenameModal] = useState<boolean>(false);
  const [deleteModal, setDeleteModal] = useState<boolean>(false);
  const [value, setValue] = useState<string>(node.id);
  const { savedSearchData }: any = useAppSelector(
    (state: any) => state.savedSearch
  );
  const [deleteTagModal, setdeleteTagModal] = useState<boolean>(false);
  const [deleteGlossaryModal, setDeleteGlossaryModal] =
    useState<boolean>(false);
  const [tagModal, setTagModal] = useState<boolean>(false);
  const [glossaryModal, setGlossaryModal] = useState<boolean>(false);
  const [termModal, setTermModal] = useState(false);
  const [categoryModal, setCategoryModal] = useState(false);

  const openNode = Boolean(expandNode);

  let selectedSearchData: any = {};

  selectedSearchData = !isEmpty(savedSearchData)
    ? savedSearchData?.find((obj: { name: string; guid: string }) => {
        if (obj.name == node.id) {
          return obj;
        }
      })
    : {};

  const handleCloseGlossaryModal = () => {
    setGlossaryModal(false);
  };
  const handleCloseTermModal = () => {
    setTermModal(false);
  };
  const handleCloseCategoryModal = () => {
    setCategoryModal(false);
  };

  const handleCloseDeleteTagModal = () => {
    setdeleteTagModal(false);
  };

  const handleCloseDeleteGlossaryModal = () => {
    setDeleteGlossaryModal(false);
  };

  const handleClickNodeMenu = (event: React.MouseEvent<HTMLElement>) => {
    event.stopPropagation();
    setExpandNode(event.currentTarget);
  };

  const handleCloseNode = () => {
    setExpandNode(null);
  };

  const handleCloseRenameModal = () => {
    setValue(selectedSearchData?.name);
    setRenameModal(false);
    setExpandNode(null);
  };
  const handleCloseTagModal = () => {
    setTagModal(false);
  };

  const handleCloseDeleteModal = () => {
    setDeleteModal(false);
    setExpandNode(null);
  };

  const handleRemove = async () => {
    try {
      await removeSavedSearch(selectedSearchData.guid);
      setDeleteModal(false);
      setExpandNode(null);
      updatedData();
      navigate(
        {
          pathname: "/search"
        },
        { replace: true }
      );
      toast.dismiss(toastId.current);
      toastId.current = toast.success(`${node.id} was deleted successfully`);
    } catch (error) {
      console.log(`Error occur while removing ${node.id}`, error);
      serverError(error, toastId);
    }
  };

  const handleEdit = async () => {
    try {
      let filterData = { ...selectedSearchData, name: value };
      await editSavedSearch(filterData as CustomFiltersNodeType, "PUT");
      updatedData();
      setRenameModal(false);
      setExpandNode(null);
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `${filterData.name} was updated successfully`
      );
    } catch (error) {
      console.log(`Error occur while updating ${node.id}`, error);
      serverError(error, toastId);
    }
  };
  return (
    <>
      {(node.types == "child" || node.types == undefined) &&
        ((treeName == "Classifications" && node.types == "child"
          ? !isEmpty(node.children)
          : isEmpty(node.children)) ||
          (treeName == "Classifications" && node.types == "child"
            ? isEmpty(node.children)
            : isEmpty(node.children))) &&
        (treeName == "CustomFilters" ||
          treeName == "Classifications" ||
          treeName == "Glossary") && (
          <IconButton
            onClick={(e) => {
              handleClickNodeMenu(e);
            }}
            size="small"
            className="tree-item-more-label"
            data-cy="dropdownMenuButton"
          >
            <MoreHorizOutlinedIcon />
          </IconButton>
        )}

      {(((treeName == "Classifications" || treeName == "Glossary") &&
        node.types == "parent" &&
        !isEmpty(node.children)) ||
        (node.types == "parent" && isEmpty(node.children)) ||
        (node.types == "child" &&
          !isEmpty(node.children) &&
          node.cGuid != undefined)) && (
        <IconButton
          onClick={(e) => {
            handleClickNodeMenu(e);
          }}
          className="tree-item-more-label"
          size="small"
          data-cy="dropdownMenuButton"
        >
          <MoreHorizOutlinedIcon className="treeitem-dropdown-toggle" />
        </IconButton>
      )}
      <Menu
        onClick={(e) => {
          e.stopPropagation();
        }}
        anchorEl={expandNode}
        id="account-menu"
        open={openNode}
        onClose={handleCloseNode}
        transformOrigin={{ horizontal: "right", vertical: "top" }}
        anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
        sx={{
          "& .MuiPaper-root": {
            transition: "none !important"
          }
        }}
      >
        {((treeName == "Classifications" &&
          !addOnClassification.includes(node.id)) ||
          (treeName == "Glossary" && node.types == "parent")) && (
          <MenuItem
            onClick={(e) => {
              e.stopPropagation();
              if (
                treeName == "Classifications" &&
                !addOnClassification.includes(node.id)
              ) {
                setTagModal(true);
              }
              if (
                treeName == "Glossary" &&
                node.types == "parent" &&
                isEmptyServicetype
              ) {
                setTermModal(true);
              }
              if (
                treeName == "Glossary" &&
                node.types == "parent" &&
                !isEmptyServicetype
              ) {
                setCategoryModal(true);
              }
            }}
            className="sidebar-menu-item"
            data-cy="createClassification"
          >
            <ListItemIcon sx={{ minWidth: "28px !important" }}>
              <AddIcon fontSize="small" className="menuitem-icon" />
            </ListItemIcon>
            <Typography
              className="menuitem-label"
              sx={{ fontSize: "0.875rem" }}
            >
              {`Create ${
                treeName == "Classifications"
                  ? "Sub-classification"
                  : isEmptyServicetype
                  ? "Term"
                  : "Category"
              }`}
            </Typography>
          </MenuItem>
        )}
        {((treeName == "Classifications" &&
          !addOnClassification.includes(node.id)) ||
          (treeName == "Glossary" && isEmptyServicetype)) && (
          <MenuItem
            onClick={(_e) => {
              if (treeName == "Classifications") {
                const searchParams = new URLSearchParams();
                searchParams.set(
                  "tag",
                  node.types == "child" ? node.label : node.nodeName
                );
                navigate({
                  pathname: `/tag/tagAttribute/${
                    node.types == "child" ? node.label : node.nodeName
                  }`,
                  search: searchParams.toString()
                });
                setExpandNode(null);
              }
              if (treeName == "Glossary" && node.types == "parent") {
                setGlossaryModal(true);
              }
              if (treeName == "Glossary" && node.types == "child") {
                const searchParams = new URLSearchParams();
                searchParams.set("gid", node.guid);
                searchParams.set("term", "term");
                searchParams.set("gtype", "term");
                searchParams.set("viewType", "term");
                searchParams.set("searchType", "basic");
                searchParams.set("term", `${node.id}@${node.parent}`);
                navigate({
                  pathname: `/glossary/${node.cGuid}`,
                  search: searchParams.toString()
                });
                setExpandNode(null);
              }
            }}
            data-cy="createClassification"
            className="sidebar-menu-item"
          >
            <ListItemIcon sx={{ minWidth: "28px !important" }}>
              <ListAltOutlinedIcon fontSize="small" className="menuitem-icon" />
            </ListItemIcon>
            <Typography
              className="menuitem-label"
              sx={{ fontSize: "0.875rem" }}
            >
              {`View/Edit ${
                treeName == "Glossary"
                  ? node.types == "parent"
                    ? "Glossary"
                    : "Term"
                  : ""
              }`}
            </Typography>
          </MenuItem>
        )}
        {((treeName == "Classifications" &&
          !addOnClassification.includes(node.id)) ||
          (treeName == "Glossary" && node.types == "parent") ||
          (treeName == "Glossary" &&
            node.types == "child" &&
            isEmptyServicetype)) && (
          // &&
          //   !isEmpty(gtype)
          <MenuItem
            onClick={(_e) => {
              if (treeName == "Classifications") {
                setdeleteTagModal(true);
              }
              if (treeName == "Glossary") {
                setDeleteGlossaryModal(true);
              }
            }}
            data-cy="createClassification"
            className="sidebar-menu-item"
          >
            <ListItemIcon sx={{ minWidth: "28px !important" }}>
              <DeleteOutlineOutlinedIcon
                fontSize="small"
                className="menuitem-icon"
              />
            </ListItemIcon>
            <Typography
              className="menuitem-label"
              sx={{ fontSize: "0.875rem" }}
            >
              {`Delete ${
                treeName == "Glossary"
                  ? node.types == "parent"
                    ? "Glossary"
                    : "Term"
                  : ""
              }`}
            </Typography>
          </MenuItem>
        )}
        {(treeName == "Classifications" ||
          (treeName == "Glossary" &&
            (node.types == "child" || node.types == undefined) &&
            isEmptyServicetype)) && (
          <MenuItem
            onClick={(e) => {
              e.stopPropagation();
              setExpandNode(null);
              const searchParams = new URLSearchParams();
              searchParams.set("searchType", "basic");
              if (treeName == "Classifications") {
                searchParams.set("tag", node.nodeName || node.id);
              } else if (treeName == "Glossary") {
                searchParams.set("term", node.id);
              }
              navigate({
                pathname: "/search/searchResult",
                search: searchParams.toString()
              });
            }}
            data-cy="createClassification"
            className="sidebar-menu-item"
          >
            <ListItemIcon sx={{ minWidth: "24px !important" }}>
              <SearchOutlinedIcon fontSize="small" className="menuitem-icon" />
            </ListItemIcon>
            <Typography
              className="menuitem-label"
              sx={{ fontSize: "0.875rem" }}
            >
              Search
            </Typography>
          </MenuItem>
        )}
        {treeName == "Glossary" &&
          !isEmptyServicetype &&
          node.types == "child" &&
          ((!isEmpty(node.children) && node.cGuid != undefined) ||
            (isEmpty(node.children) &&
              (node.cGuid != undefined || node.cGuid == undefined))) && (
            <MenuItem
              onClick={(_e) => {
                setCategoryModal(true);
              }}
              data-cy="createClassification"
              className="sidebar-menu-item"
            >
              <ListItemIcon sx={{ minWidth: "24px !important" }}>
                <ListAltIcon fontSize="small" className="menuitem-icon" />
              </ListItemIcon>
              <Typography
                className="menuitem-label"
                sx={{ fontSize: "0.875rem" }}
              >
                Create Sub-Category
              </Typography>
            </MenuItem>
          )}
        {treeName == "CustomFilters" && (
          <MenuItem
            onClick={(e) => {
              e.stopPropagation();
              setRenameModal(true);
            }}
            data-cy="createClassification"
            className="sidebar-menu-item"
          >
            <ListItemIcon>
              <EditOutlinedIcon fontSize="small" className="menuitem-icon" />
            </ListItemIcon>
            <Typography className="menuitem-label">Rename</Typography>
          </MenuItem>
        )}

        {treeName == "CustomFilters" && (
          <MenuItem
            onClick={(e) => {
              e.stopPropagation();
              setDeleteModal(true);
            }}
            data-cy="downloadBusinessMetadata"
            className="sidebar-menu-item"
          >
            <ListItemIcon>
              <DeleteOutlineOutlinedIcon
                fontSize="small"
                className="menuitem-icon"
              />
            </ListItemIcon>
            <Typography className="menuitem-label">Delete</Typography>
          </MenuItem>
        )}
      </Menu>
      <CustomModal
        open={renameModal}
        onClose={handleCloseRenameModal}
        title={`Rename ${node.parent} Custom Filter`}
        titleIcon={
          <EditOutlinedIcon fontSize="small" className="menuitem-icon" />
        }
        button1Label="Cancel"
        button1Handler={handleCloseRenameModal}
        button2Label="Update"
        button2Handler={handleEdit}
        // disableButton2={value}
      >
        <Stack
          sx={{
            width: 500,
            maxWidth: "100%"
          }}
        >
          <TextField
            value={value}
            required
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              e.stopPropagation();
              setValue(e.target.value);
            }}
            onClick={(e: any) => {
              e.stopPropagation();
              e.isDefaultPrevented();
            }}
            fullWidth
            defaultValue={node.id}
            label="Name"
            size="small"
          />
        </Stack>
      </CustomModal>
      <CustomModal
        open={deleteModal}
        onClose={handleCloseDeleteModal}
        title="Confirmation"
        titleIcon={<ErrorRoundedIcon className="remove-modal-icon" />}
        button1Label="Cancel"
        button1Handler={handleCloseDeleteModal}
        button2Label="Ok"
        button2Handler={handleRemove}
      >
        <Typography fontSize={15}>
          Are you sure you want to delete <strong>{node.id}</strong> ?{""}
        </Typography>
      </CustomModal>

      {deleteTagModal && (
        <DeleteTag
          open={deleteTagModal}
          onClose={handleCloseDeleteTagModal}
          setExpandNode={setExpandNode}
          node={node}
          updatedData={updatedData}
        />
      )}
      {deleteGlossaryModal && (
        <DeleteGlossary
          open={deleteGlossaryModal}
          onClose={handleCloseDeleteGlossaryModal}
          setExpandNode={setExpandNode}
          node={node}
          updatedData={updatedData}
        />
      )}
      {tagModal && (
        <ClassificationForm
          open={tagModal}
          isAdd={true}
          subAdd={true}
          onClose={handleCloseTagModal}
          node={node}
        />
      )}

      {glossaryModal && node.types == "parent" && (
        <AddUpdateGlossaryForm
          open={glossaryModal}
          isAdd={false}
          onClose={handleCloseGlossaryModal}
          node={node}
        />
      )}
      {termModal && (
        <AddUpdateTermForm
          open={termModal}
          isAdd={true}
          onClose={handleCloseTermModal}
          node={node}
          dataObj={undefined}
        />
      )}
      {categoryModal && (
        <AddUpdateCategoryForm
          open={categoryModal}
          isAdd={true}
          onClose={handleCloseCategoryModal}
          node={node}
          dataObj={undefined}
        />
      )}
    </>
  );
};

export default TreeNodeIcons;
