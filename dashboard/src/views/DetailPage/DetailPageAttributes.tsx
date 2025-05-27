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

import { CustomButton, LightTooltip } from "@components/muiComponents";
import SkeletonLoader from "@components/SkeletonLoader";
import {
  Divider,
  IconButton,
  Stack,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
  Autocomplete,
  TextField,
  Menu,
  MenuItem
} from "@mui/material";
import {
  extractKeyValueFromEntity,
  isEmpty,
  sanitizeHtmlContent
} from "@utils/Utils";

const getDescriptionForDisplay = (desc: unknown): string => {
  if (typeof desc === "string") return desc;
  if (desc && typeof desc === "object" && !Array.isArray(desc)) {
    const val = Object.values(desc).find((v) => typeof v === "string");
    return (val as string) || "";
  }
  return "";
};
import { useState, useMemo } from "react";
import { useAppSelector } from "@hooks/reducerHook";
import { toast } from "react-toastify";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import DeleteOutlinedIcon from "@mui/icons-material/DeleteOutlined";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import { useParams, useSearchParams, Link } from "react-router-dom";
import { TableLayout } from "@components/Table/TableLayout";
import ClassificationForm from "@views/Classification/ClassificationForm";
import AddUpdateTermForm from "@views/Glossary/AddUpdateTermForm";
import AddUpdateCategoryForm from "@views/Glossary/AddUpdateCategoryForm";
import { removeTermorCategory, getGlossaryType } from "@api/apiMethods/glossaryApiMethod";
import { StyledPaper, Item } from "@utils/Muiutils";
import AddUpdateGlossaryForm from "@views/Glossary/AddUpdateGlossaryForm";
import ShowMoreView from "@components/ShowMore/ShowMoreView";
import AddCircleOutlineIcon from "@mui/icons-material/AddCircleOutline";
import AddTag from "@views/Classification/AddTag";
import AddTagAttributes from "@views/Classification/AddTagAttributes";
import AssignCategory from "@views/Glossary/AssignCategory";
import AssignTerm from "@views/Glossary/AssignTerm";
import DeleteGlossary from "@views/Glossary/DeleteGlossary";
import ShowMoreText from "@components/ShowMore/ShowMoreText";


const DetailPageAttribute = ({
  data,
  description,
  subTypes,
  superTypes,
  loading,
  shortDescription,
  attributeDefs
}: any) => {
  const [searchParams] = useSearchParams();
  const { guid, tagName, bmguid } = useParams();
  const gtypeParams = searchParams.get("gtype");

  const [alignment, setAlignment] = useState<string>("formatted");
  const [tagModal, setTagModal] = useState<boolean>(false);
  const [editTermModal, setEditTermModal] = useState(false);
  const [editCategoryModal, setEditCategoryModal] = useState(false);

  const [openAddTagModal, setOpenAddTagModal] = useState<boolean>(false);
  const [attributeModal, setAttributeModal] = useState<boolean>(false);
  const [openAddTermModal, setOpenAddTermModal] = useState<boolean>(false);

  const [categoryModal, setCategoryModal] = useState<boolean>(false);
  const [editGlossaryModal, setEditGlossaryModal] = useState<boolean>(false);

  const [selectedRowItem, setSelectedRowItem] = useState<any>(null);
  const [editRowModal, setEditRowModal] = useState<boolean>(false);
  const [deleteRowModal, setDeleteRowModal] = useState<boolean>(false);
  const [createTermModal, setCreateTermModal] = useState<boolean>(false);
  const [createCategoryModal, setCreateCategoryModal] = useState<boolean>(false);
  const [createAnchorEl, setCreateAnchorEl] = useState<null | HTMLElement>(null);
  const openCreateMenu = Boolean(createAnchorEl);

  const handleCreateClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setCreateAnchorEl(event.currentTarget);
  };
  const handleCreateClose = () => {
    setCreateAnchorEl(null);
  };


  const [isTableLoading, setIsTableLoading] = useState<boolean>(false);
  const [localTerms, setLocalTerms] = useState<any[]>(data?.terms || []);
  const [localCategories, setLocalCategories] = useState<any[]>(data?.categories || []);

  const [prevData, setPrevData] = useState<any>(data);
  if (data !== prevData) {
    setPrevData(data);
    setLocalTerms(data?.terms || []);
    setLocalCategories(data?.categories || []);
  }


  const handleTableUpdate = async (showLoader = true) => {
    if (gtypeParams && guid) {
      if (showLoader) setIsTableLoading(true);
      try {
        const res = await getGlossaryType(gtypeParams, guid);
        setLocalTerms(res.data?.terms || []);
        setLocalCategories(res.data?.categories || []);
      } finally {
        if (showLoader) setIsTableLoading(false);
      }
    }
  };

  const [glossaryFilter, setGlossaryFilter] = useState<string>("All");

  const glossaryTermsAndCategories = useMemo(() => {
    if (gtypeParams !== "glossary") return [];
    const t = (localTerms || []).map((t: any) => ({ ...t, _itemType: "term" }));
    const c = (localCategories || []).map((c: any) => ({ ...c, _itemType: "category" }));
    return [...t, ...c];
  }, [localTerms, localCategories, gtypeParams]);

  const filteredGlossaryItems = useMemo(() => {
    if (glossaryFilter === "All") return glossaryTermsAndCategories;
    if (glossaryFilter === "Terms") return glossaryTermsAndCategories.filter((i: any) => i._itemType === "term");
    if (glossaryFilter === "Categories") return glossaryTermsAndCategories.filter((i: any) => i._itemType === "category");
    return glossaryTermsAndCategories;
  }, [glossaryTermsAndCategories, glossaryFilter]);

  const filterOptions = ["All", "Terms", "Categories"];

  const glossaryTableColumns = useMemo(() => [
    {
      accessorKey: "displayText",
      header: "Name",
      cell: (info: any) => {
        const item = info.row.original;
        const type = item._itemType;
        const targetGuid = item.termGuid || item.categoryGuid || item.guid || item.id;
        const href = `/glossary/${targetGuid}?gtype=${type}&view=properties`;
        return (
          <Link to={href} className="text-blue text-decoration-none fw-600">
            {item.displayText}
          </Link>
        );
      }
    },
    {
      accessorKey: "glossaryType",
      header: "Glossary Type",
      enableSorting: false,
      cell: (info: any) => {
        const type = info.row.original._itemType;
        return type ? type.charAt(0).toUpperCase() + type.slice(1) : "N/A";
      }
    },
    {
      accessorKey: "action",
      header: "Action",
      enableSorting: false,
      cell: (info: any) => {
        const item = info.row.original;
        const type = item._itemType;
        return (
          <Stack direction="row" gap={1}>
            <LightTooltip title={`Edit ${type === "term" ? "Term" : "Category"}`}>
              <CustomButton
                variant="outlined"
                color="success"
                className="table-filter-btn assignTag"
                size="small"
                onClick={async (e: React.MouseEvent<HTMLElement>) => {
                  e.stopPropagation();
                  try {
                    const targetGuid = item.termGuid || item.categoryGuid || item.guid || item.id;
                    const res = await getGlossaryType(type, targetGuid);
                    setSelectedRowItem({ ...item, ...res.data });
                    setEditRowModal(true);
                  } catch (err) {
                    toast.error("Failed to fetch details for editing");
                  }
                }}
              >
                <EditOutlinedIcon className="table-filter-refresh" />
              </CustomButton>
            </LightTooltip>

            <LightTooltip title={`Delete ${type === "term" ? "Term" : "Category"}`}>
              <CustomButton
                variant="outlined"
                color="success"
                className="table-filter-btn assignTag"
                size="small"
                onClick={(e: React.MouseEvent<HTMLElement>) => {
                  e.stopPropagation();
                  setSelectedRowItem(item);
                  setDeleteRowModal(true);
                }}
              >
                <DeleteOutlinedIcon className="table-filter-refresh" />
              </CustomButton>
            </LightTooltip>
          </Stack>
        );
      }
    }
  ], []);

  const handleCloseEditGlossaryModal = () => {
    setEditGlossaryModal(false);
  };

  const handleCloseTermModal = () => {
    setOpenAddTermModal(false);
  };
  const handleCloseAddTagModal = () => {
    setOpenAddTagModal(false);
  };
  const handleCloseCategoryModal = () => {
    setCategoryModal(false);
  };
  const handleCloseAttributeModal = () => {
    setAttributeModal(false);
  };
  const handleChange = (
    _event: React.MouseEvent<HTMLElement>,
    newAlignment: string
  ) => {
    setAlignment(newAlignment);
  };

  const handleCloseTagModal = () => {
    setTagModal(false);
  };

  const handleCloseEditTermModal = () => {
    setEditTermModal(false);
  };

  const handleCloseEditCategoryModal = () => {
    setEditCategoryModal(false);
  };

  const { name }: { name: string; found: boolean; key: any } =
    extractKeyValueFromEntity(data);

  const { glossaryData }: any = useAppSelector((state: any) => state.glossary);
  const hasAnyGlossaryTerms = Array.isArray(glossaryData)
    ? glossaryData.some((g: any) => Array.isArray(g?.terms) && g.terms.length > 0)
    : false;

  return (
    <>
      <Stack
        direction="column"
        justifyContent="center"
        alignItems="stretch"
        spacing={1}
        minHeight="100%"
      >
        <StyledPaper
          sx={{ display: "flex", flexDirection: "column", gap: "1rem" }}
          className="detail-page-paper"
          variant="outlined"
        >
          <Stack direction="row" justifyContent="space-between">
            <Typography
              noWrap
              fontWeight={600}
              fontSize={"24px"}
              component={"h1"}
              data-id="title"
              className="detail-page-enity-name mb-0 mt-0"
            >
              {name}{" "}
            </Typography>
            {isEmpty(bmguid) && (
              <LightTooltip
                title={
                  !isEmpty(tagName)
                    ? "Edit Classification"
                    : !isEmpty(guid) && gtypeParams == "term"
                      ? "Edit Term"
                      : !isEmpty(guid) && gtypeParams == "category"
                        ? "Edit Category"
                        : !isEmpty(guid) && gtypeParams == "glossary"
                          ? "Edit Glossary"
                          : "Edit"
                }
              >
                <CustomButton
                  variant="outlined"
                  color="success"
                  className="table-filter-btn assignTag"
                  size="small"
                  onClick={() => {
                    if (!isEmpty(tagName)) {
                      setTagModal(true);
                    }
                    if (!isEmpty(guid) && gtypeParams == "term") {
                      setEditTermModal(true);
                    }
                    if (!isEmpty(guid) && gtypeParams == "category") {
                      setEditCategoryModal(true);
                    }
                    if (!isEmpty(guid) && gtypeParams == "glossary") {
                      setEditGlossaryModal(true);
                    }
                  }}
                  data-cy="addTag"
                >
                  <EditOutlinedIcon className="table-filter-refresh" />
                </CustomButton>
              </LightTooltip>
            )}
          </Stack>
          <Divider />
          <div
            style={{
              ...(isEmpty(bmguid)
                ? {
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr",
                  gridGap: "1rem 2rem",
                  marginBottom: "0.75rem"
                }
                : {})
            }}
          >
            {" "}
            {shortDescription != undefined && (
              <>
                <Stack gap="0.5rem" direction="row">
                  <Typography
                    flexBasis="12%"
                    fontWeight="600"
                    className="opacity-07"
                    fontSize={"16px"}
                  >
                    Short Description
                  </Typography>

                  <div
                    style={{
                      wordBreak: "break-word"
                    }}
                  >
                    {!isEmpty(shortDescription) ? (
                      <ShowMoreText
                        value={getDescriptionForDisplay(shortDescription)}
                        maxLength={160}
                        more={"show more"}
                        less={"show less"}
                      />
                    ) : (
                      "N/A"
                    )}
                  </div>
                </Stack>
              </>
            )}{" "}
            <Stack direction="column" gap="0.375rem">
              <Stack
                direction="row"
                gap="1rem"
                alignItems="center"
                position="relative"
              >
                <Typography
                  fontWeight="600"
                  className="opacity-07"
                  fontSize={"16px"}
                >
                  {shortDescription != undefined ? `Long` : ""} Description
                </Typography>
                <ToggleButtonGroup
                  size="small"
                  color="primary"
                  value={alignment}
                  exclusive
                  onChange={handleChange}
                  aria-label="Platform"
                >
                  <ToggleButton
                    className="entity-form-toggle-btn"
                    size="small"
                    value="formatted"
                    data-cy="formatted"
                    sx={{ padding: "4px 8px" }}
                  >
                    Formatted
                  </ToggleButton>
                  <ToggleButton
                    size="small"
                    value="plain"
                    className="entity-form-toggle-btn detailpage-desc-toggle"
                    data-cy="plain"
                    sx={{ padding: "4px 8px" }}
                  >
                    Plain
                  </ToggleButton>
                </ToggleButtonGroup>
              </Stack>
              <div
                className="flex-1"
                style={{
                  paddingRight: "1rem"
                }}
              >
                <div>
                  {alignment == "formatted" ? (
                    <ShowMoreText
                      value={sanitizeHtmlContent(getDescriptionForDisplay(description))}
                      maxLength={160}
                      more={"show more"}
                      less={"show less"}
                      isHtml={true}
                    />
                  ) : (
                    <div style={{ wordBreak: "break-all" }}>
                      <ShowMoreText
                        value={sanitizeHtmlContent(getDescriptionForDisplay(description))}
                        maxLength={160}
                        more={"show more"}
                        less={"show less"}
                      />
                    </div>
                  )}
                </div>
              </div>
            </Stack>
            {!isEmpty(gtypeParams) &&
              (gtypeParams == "term" || gtypeParams == "category") &&
              (loading ? (
                <Stack direction="column" spacing={2} alignItems="left">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={300}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                gtypeParams != "category" && (
                  <Stack
                    direction="column"
                    alignItems="flex-start"
                    gap="0.5rem"
                  >
                    <Stack direction="row" alignItems="center" gap="0.75rem">
                      <Typography
                        lineHeight="26px"
                        flexBasis="12%"
                        fontWeight="600"
                        fontSize="16px"
                        className="entity-attribute-label"
                      >
                        Classifications
                      </Typography>
                      <LightTooltip title={"Add Classifications"}>
                        <IconButton
                          component="label"
                          role={undefined}
                          tabIndex={-1}
                          size="small"
                          color="primary"
                          onClick={() => {
                            setOpenAddTagModal(true);
                          }}
                        >
                          <AddCircleOutlineIcon
                            className="mr-0"
                            fontSize="small"
                          />{" "}
                        </IconButton>
                      </LightTooltip>
                    </Stack>
                    <Stack
                      data-cy="tagListTerm"
                      direction="row"
                      flex="1"
                      justifyContent="flex-start"
                    >
                      <ShowMoreView
                        data={data?.["classifications"] || []}
                        maxVisible={4}
                        title="Classifications"
                        displayKey="typeName"
                        removeApiMethod={removeClassification}
                        removeTagsTitle={"Remove Classification Assignment"}
                        currentEntity={data}
                        isEditView={false}
                        id={"Classifications"}
                      />
                    </Stack>
                  </Stack>
                )
              ))}
            {!isEmpty(gtypeParams) &&
              (gtypeParams == "category") &&
              (loading ? (
                <Stack direction="column" spacing={2} alignItems="left">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={300}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                !isEmpty(guid) &&
                (gtypeParams == "category") && (
                  <Stack
                    direction="column"
                    alignItems="flex-start"
                    gap="0.5rem"
                  >
                    <Stack direction="row" alignItems="center" gap="0.75rem">
                      <Typography
                        lineHeight="26px"
                        flexBasis="12%"
                        fontWeight="600"
                        className="entity-attribute-label"
                        fontSize="16px"
                      >
                        Terms
                      </Typography>
                      <LightTooltip title={"Add Term"}>
                        <IconButton
                          component="label"
                          role={undefined}
                          tabIndex={-1}
                          size="small"
                          color="primary"
                          onClick={() => {
                            if (!hasAnyGlossaryTerms) {
                              toast.dismiss();
                              toast.info("There are no available terms");
                              return;
                            }
                            setOpenAddTermModal(true);
                          }}
                        >
                          <AddCircleOutlineIcon
                            className="mr-0"
                            fontSize="small"
                          />{" "}
                        </IconButton>
                      </LightTooltip>
                    </Stack>
                    <Stack
                      data-cy="termList"
                      direction="row"
                      flex="1"
                      justifyContent="flex-start"
                    >
                      {isEmpty(data?.["terms"]) ? (
                        <Typography lineHeight="26px" fontSize="14px">N/A</Typography>
                      ) : (
                        <ShowMoreView
                          data={data?.["terms"] || []}
                          maxVisible={4}
                          title="Terms"
                          displayKey="displayText"
                          isEditView={false}
                          currentEntity={data}
                          removeApiMethod={removeTermorCategory}
                          removeTagsTitle={"Remove Term Assignment"}
                          id={"Terms"}
                        />
                      )}
                    </Stack>
                  </Stack>
                )
              ))}
            {!isEmpty(gtypeParams) &&
              gtypeParams == "term" &&
              (loading ? (
                <Stack direction="column" spacing={2} alignItems="left">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={300}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                !isEmpty(guid) && (
                  <Stack
                    direction="column"
                    alignItems="flex-start"
                    gap="0.5rem"
                  >
                    <Stack direction="row" alignItems="center" gap="0.75rem">
                      <Typography
                        flexBasis="12%"
                        fontWeight="600"
                        className="entity-attribute-label"
                        fontSize={"16px"}
                      >
                        Categories
                      </Typography>
                      <LightTooltip title={"Add Categories"}>
                        <IconButton
                          component="label"
                          role={undefined}
                          tabIndex={-1}
                          size="small"
                          color="primary"
                          onClick={() => {
                            setCategoryModal(true);
                          }}
                        >
                          <AddCircleOutlineIcon
                            className="mr-0"
                            fontSize="small"
                          />{" "}
                        </IconButton>
                      </LightTooltip>
                    </Stack>
                    <Stack
                      data-cy="categoryList"
                      direction="row"
                      flex="1"
                      justifyContent="flex-start"
                    >
                      {isEmpty(data?.["categories"]) ? (
                        <Typography lineHeight="26px" fontSize="14px">N/A</Typography>
                      ) : (
                        <ShowMoreView
                          data={data?.["categories"] || []}
                          maxVisible={4}
                          title="Category"
                          displayKey="displayText"
                          removeApiMethod={removeTermorCategory}
                          currentEntity={data}
                          removeTagsTitle={"Remove Category Assignment"}
                          isEditView={false}
                          id={"Category"}
                        />
                      )}
                    </Stack>
                  </Stack>
                )
              ))}
            {!isEmpty(superTypes) && (
              <>
                {loading ? (
                  <Stack direction="column" spacing={2} alignItems="left">
                    <SkeletonLoader
                      count={1}
                      variant="text"
                      width={300}
                      className="text-loader"
                    />
                  </Stack>
                ) : (
                  <Stack
                    direction="column"
                    alignItems="flex-start"
                    gap="0.5rem"
                  >
                    <Stack direction="row" alignItems="center" gap="0.75rem">
                      <Typography
                        fontWeight="600"
                        className="entity-attribute-label"
                        fontSize={"16px"}
                      >
                        Direct super-classifications
                      </Typography>
                    </Stack>
                    <Stack
                      data-cy="direct-super-classifications"
                      direction="row"
                      flex="1"
                      justifyContent="flex-start"
                    >
                      <ShowMoreView
                        data={data["superTypes"] || []}
                        maxVisible={4}
                        title="Super Classifications"
                        displayKey=""
                        currentEntity={data}
                        isEditView={false}
                        id={"Super Classifications"}
                      />
                    </Stack>
                  </Stack>
                )}
              </>
            )}
            {!isEmpty(subTypes) && (
              <>
                {loading ? (
                  <Stack direction="column" spacing={2} alignItems="left">
                    <SkeletonLoader
                      count={1}
                      variant="text"
                      width={300}
                      className="text-loader"
                    />
                  </Stack>
                ) : (
                  <Stack
                    direction="column"
                    alignItems="flex-start"
                    gap="0.5rem"
                  >
                    <Stack direction="row" alignItems="center" gap="0.75rem">
                      <Typography
                        fontWeight="600"
                        className="entity-attribute-label"
                        fontSize={"16px"}
                      >
                        Direct sub-classifications
                      </Typography>
                    </Stack>
                    <Stack
                      data-cy="propagatedTagList"
                      direction="row"
                      flex="1"
                      justifyContent="flex-start"
                    >
                      <ShowMoreView
                        data={data?.["subTypes"] || []}
                        maxVisible={4}
                        title="Sub Classifications"
                        displayKey=""
                        currentEntity={data}
                        isEditView={false}
                        id={"Sub Classifications"}
                      />
                    </Stack>
                  </Stack>
                )}
              </>
            )}
            {attributeDefs != undefined && (
              <>
                <>
                  {loading ? (
                    <Stack direction="column" spacing={2} alignItems="left">
                      <SkeletonLoader
                        count={1}
                        variant="text"
                        width={300}
                        className="text-loader"
                      />
                    </Stack>
                  ) : (
                    <Stack
                      direction="column"
                      alignItems="flex-start"
                      gap="0.5rem"
                    >
                      <Stack direction="row" alignItems="center" gap="0.75rem">
                        <Typography
                          lineHeight="26px"
                          flexBasis="12%"
                          fontWeight="600"
                          className="entity-attribute-label"
                          fontSize={"16px"}
                        >
                          Attributes:
                        </Typography>
                        <LightTooltip title={"Add Attributes"}>
                          <IconButton
                            component="label"
                            role={undefined}
                            tabIndex={-1}
                            size="small"
                            color="primary"
                            onClick={() => {
                              setAttributeModal(true);
                            }}
                          >
                            <AddCircleOutlineIcon
                              className="mr-0"
                              fontSize="small"
                            />{" "}
                          </IconButton>
                        </LightTooltip>
                      </Stack>

                      <Stack
                        data-cy="propagatedTagList"
                        direction="row"
                        flex="1"
                        justifyContent="flex-start"
                      >
                        <ShowMoreView
                          data={data?.["attributeDefs"] || []}
                          maxVisible={4}
                          title="Atrributes"
                          displayKey="name"
                          currentEntity={data}
                          isEditView={false}
                          id={"Atrributes"}
                        />
                      </Stack>
                    </Stack>
                  )}
                </>
              </>
            )}
          </div>
        </StyledPaper>
      </Stack>

      {!isEmpty(gtypeParams) && gtypeParams == "glossary" && (
        <Item variant="outlined" className="glossary-detail-items" sx={{ mt: 2, p: 2 }}>
          <Stack>
            <Stack
              direction="row"
              justifyContent="space-between"
              alignItems="center"
              marginBottom="0.5rem"
              gap="1rem"
            >
              <Autocomplete
                size="small"
                disablePortal
                options={filterOptions}
                value={glossaryFilter}
                onChange={(_e, newVal) => setGlossaryFilter(newVal as string)}
                isOptionEqualToValue={(option, value) => option === value}
                getOptionLabel={(option) => option}
                disableClearable
                className="classification-table-autocomplete"
                renderInput={(params) => (
                  <TextField
                    {...params}
                    label="Terms and Categories"
                  />
                )}
              />
              {(gtypeParams === "glossary" || gtypeParams === "category") && (
                <>
                  <CustomButton
                    variant="contained"
                    size="small"
                    onClick={handleCreateClick}
                    endIcon={<KeyboardArrowDownIcon />}
                  >
                    Create
                  </CustomButton>
                  <Menu
                    anchorEl={createAnchorEl}
                    open={openCreateMenu}
                    onClose={handleCreateClose}
                    anchorOrigin={{
                      vertical: 'bottom',
                      horizontal: 'right',
                    }}
                    transformOrigin={{
                      vertical: 'top',
                      horizontal: 'right',
                    }}
                  >
                    <MenuItem onClick={() => { setCreateCategoryModal(true); handleCreateClose(); }}>Category</MenuItem>
                    <MenuItem onClick={() => { setCreateTermModal(true); handleCreateClose(); }}>Term</MenuItem>
                  </Menu>
                </>
              )}
            </Stack>
            <TableLayout
              data={filteredGlossaryItems}
              columns={glossaryTableColumns}
              columnVisibility={false}
              columnSort={true}
              showPagination={true}
              isFetching={loading || isTableLoading}
              showRowSelection={false}
              clientSideSorting={true}
              isClientSidePagination={true}
              emptyText="No Records found!"
            />
          </Stack>
        </Item>
      )}

      {tagModal && (
        <ClassificationForm
          open={tagModal}
          onClose={handleCloseTagModal}
          setTagModal={setTagModal}
        />
      )}
      {openAddTagModal && (
        <AddTag
          open={openAddTagModal}
          isAdd={true}
          entityData={data}
          onClose={handleCloseAddTagModal}
          setUpdateTable={undefined}
          setRowSelection={undefined}
        />
      )}
      {editTermModal && (
        <AddUpdateTermForm
          open={editTermModal}
          isAdd={false}
          onClose={handleCloseEditTermModal}
          node={undefined}
          dataObj={data}
        />
      )}
      {editCategoryModal && (
        <AddUpdateCategoryForm
          open={editCategoryModal}
          isAdd={false}
          onClose={handleCloseEditCategoryModal}
          node={undefined}
          dataObj={data}
        />
      )}
      {editGlossaryModal && (
        <AddUpdateGlossaryForm
          open={editGlossaryModal}
          isAdd={false}
          onClose={handleCloseEditGlossaryModal}
          node={{ id: data?.name }}
          dataObj={data}
        />
      )}
      {attributeModal && (
        <AddTagAttributes
          open={attributeModal}
          onClose={handleCloseAttributeModal}
        />
      )}
      {categoryModal && (
        <AssignCategory
          open={categoryModal}
          onClose={handleCloseCategoryModal}
          data={data || {}}
          updateTable={undefined}
        />
      )}

      {openAddTermModal && (
        <AssignTerm
          open={openAddTermModal}
          onClose={handleCloseTermModal}
          data={data || {}}
          updateTable={undefined}
          relatedTerm={undefined}
        />
      )}

      {editRowModal && selectedRowItem?._itemType === "term" && (
        <AddUpdateTermForm
          open={editRowModal}
          isAdd={false}
          onClose={() => setEditRowModal(false)}
          node={undefined}
          dataObj={{
            ...selectedRowItem,
            guid: selectedRowItem.termGuid || selectedRowItem.guid || selectedRowItem.id,
            name: selectedRowItem.displayText,
            shortDescription: selectedRowItem.shortDescription || selectedRowItem.description || "",
            longDescription: selectedRowItem.longDescription || "",
            anchor: {
              glossaryGuid: gtypeParams === "glossary" ? (data?.guid || guid) : data?.anchor?.glossaryGuid,
              displayText: gtypeParams === "glossary" ? data?.name : data?.anchor?.displayText
            }
          }}
        />
      )}

      {editRowModal && selectedRowItem?._itemType === "category" && (
        <AddUpdateCategoryForm
          open={editRowModal}
          isAdd={false}
          onClose={() => setEditRowModal(false)}
          node={undefined}
          dataObj={{
            ...selectedRowItem,
            guid: selectedRowItem.categoryGuid || selectedRowItem.guid || selectedRowItem.id,
            name: selectedRowItem.displayText,
            shortDescription: selectedRowItem.shortDescription || selectedRowItem.description || "",
            longDescription: selectedRowItem.longDescription || "",
            anchor: {
              glossaryGuid: gtypeParams === "glossary" ? (data?.guid || guid) : data?.anchor?.glossaryGuid,
              displayText: gtypeParams === "glossary" ? data?.name : data?.anchor?.displayText
            }
          }}
        />
      )}

      {deleteRowModal && (
        <DeleteGlossary
          open={deleteRowModal}
          onClose={() => setDeleteRowModal(false)}
          setExpandNode={undefined}
          node={{
            id: selectedRowItem?.displayText,
            guid: selectedRowItem?.termGuid || selectedRowItem?.guid || selectedRowItem?.id,
            cGuid: selectedRowItem?.termGuid || selectedRowItem?.categoryGuid || selectedRowItem?.guid || selectedRowItem?.id,
            types: selectedRowItem?._itemType === "category" ? "Category" : "Term"
          }}
          updatedData={handleTableUpdate}
        />
      )}
      {createTermModal && (
        <AddUpdateTermForm
          open={createTermModal}
          isAdd={true}
          onClose={() => setCreateTermModal(false)}
          node={{
            id: data?.name,
            parent: gtypeParams === "glossary" ? data?.name : data?.anchor?.displayText,
            guid: data?.guid || guid,
            cGuid: data?.guid || guid,
            types: gtypeParams === "glossary" ? "Glossary" : "Category"
          }}
          dataObj={undefined}
        />
      )}
      {createCategoryModal && (
        <AddUpdateCategoryForm
          open={createCategoryModal}
          isAdd={true}
          onClose={() => setCreateCategoryModal(false)}
          node={{
            id: data?.name,
            parent: gtypeParams === "glossary" ? data?.name : data?.anchor?.displayText,
            guid: data?.guid || guid,
            cGuid: data?.guid || guid,
            types: gtypeParams === "glossary" ? "Glossary" : "Category"
          }}
          dataObj={undefined}
        />
      )}
    </>
  );
};

export default DetailPageAttribute;
