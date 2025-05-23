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
  Typography
} from "@mui/material";
import {
  extractKeyValueFromEntity,
  isEmpty,
  sanitizeHtmlContent
} from "@utils/Utils";
import { useState } from "react";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import { useParams, useSearchParams } from "react-router-dom";
import ClassificationForm from "@views/Classification/ClassificationForm";
import AddUpdateTermForm from "@views/Glossary/AddUpdateTermForm";
import AddUpdateCategoryForm from "@views/Glossary/AddUpdateCategoryForm";
import { removeTermorCategory } from "@api/apiMethods/glossaryApiMethod";
import { StyledPaper } from "@utils/Muiutils";
import ShowMoreView from "@components/ShowMore/ShowMoreView";
import AddCircleOutlineIcon from "@mui/icons-material/AddCircleOutline";
import AddTag from "@views/Classification/AddTag";
import AddTagAttributes from "@views/Classification/AddTagAttributes";
import AssignCategory from "@views/Glossary/AssignCategory";
import AssignTerm from "@views/Glossary/AssignTerm";
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
              <LightTooltip title={"Edit Classification"}>
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
                <Stack gap="0.5rem">
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
                        value={shortDescription}
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
                <Stack gap={1} right="0" top="0" justifyContent="flex-end">
                  {alignment == "formatted" ? (
                    <div style={{ wordBreak: "break-all" }}>
                      <ShowMoreText
                        value={sanitizeHtmlContent(description)}
                        maxLength={160}
                        more={"show more"}
                        less={"show less"}
                        isHtml={true}
                      />
                    </div>
                  ) : (
                    <div style={{ wordBreak: "break-all" }}>
                      <ShowMoreText
                        value={sanitizeHtmlContent(description)}
                        maxLength={160}
                        more={"show more"}
                        less={"show less"}
                      />
                    </div>
                  )}
                </Stack>
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
              gtypeParams == "category" &&
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
                gtypeParams == "category" && (
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
                !isEmpty(guid) &&
                gtypeParams == "term" && (
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
    </>
  );
};

export default DetailPageAttribute;
