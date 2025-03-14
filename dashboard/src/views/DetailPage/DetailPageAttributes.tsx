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

import DialogShowMoreLess from "@components/DialogShowMoreLess";
import { CustomButton, LightTooltip } from "@components/muiComponents";
import SkeletonLoader from "@components/SkeletonLoader";
import {
  Divider,
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

const DetailPageAttribute = ({
  data,
  description,
  subTypes,
  superTypes,
  entityTypes,
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
            <h1 className="detail-page-enity-name mb-0 mt-0"> {name}</h1>
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
          {shortDescription != undefined && (
            <>
              <Stack direction="row" gap="0.5rem">
                <Typography
                  flexBasis="12%"
                  fontWeight="800"
                  className="opacity-05"
                >
                  Short Description:
                </Typography>

                <Typography flex="1" sx={{ wordBreak: "break-all" }}>
                  {!isEmpty(shortDescription) ? shortDescription : "N/A"}
                </Typography>
              </Stack>
              <Divider />
            </>
          )}{" "}
          <Stack direction="column" gap="0.375rem">
            <Stack
              direction="row"
              gap="0.5rem"
              justifyContent="space-between"
              alignItems="flex-start"
            >
              <Typography
                flexBasis="12%"
                fontWeight="800"
                className="opacity-05"
              >
                {shortDescription != undefined ? `Long` : ""} Description:
              </Typography>
              <div className="flex-1" style={{ paddingRight: "1rem" }}>
                <Stack gap={1} justifyContent="flex-end">
                  {alignment == "formatted" ? (
                    <div
                      className="long-descriptions"
                      dangerouslySetInnerHTML={{
                        __html: sanitizeHtmlContent(description)
                      }}
                      data-id="longDescription"
                    />
                  ) : (
                    sanitizeHtmlContent(description)
                  )}
                </Stack>
              </div>
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
                >
                  Formatted
                </ToggleButton>
                <ToggleButton
                  size="small"
                  value="plain"
                  className="entity-form-toggle-btn detailpage-desc-toggle"
                  data-cy="plain"
                >
                  Plain
                </ToggleButton>
              </ToggleButtonGroup>
            </Stack>
          </Stack>
          <Divider />
          {!isEmpty(gtypeParams) &&
            (gtypeParams == "term" || gtypeParams == "category") && (
              <>
                {loading ? (
                  <Stack direction="row" spacing={2} alignItems="center">
                    <SkeletonLoader
                      count={1}
                      variant="text"
                      width={200}
                      className="text-loader"
                    />
                    <SkeletonLoader
                      count={1}
                      variant="text"
                      width={1000}
                      className="text-loader"
                    />
                  </Stack>
                ) : (
                  gtypeParams != "category" && (
                    <Stack direction="row">
                      <Typography
                        flexBasis="12%"
                        fontWeight="800"
                        className="entity-attribute-label"
                      >
                        Classifications:
                      </Typography>
                      <Stack
                        data-cy="tagListTerm"
                        direction="row"
                        flex="1"
                        justifyContent="flex-start"
                      >
                        <DialogShowMoreLess
                          value={data}
                          readOnly={false}
                          // setUpdateTable={setUpdateTable}
                          columnVal="classifications"
                          colName="Classification"
                          displayText="typeName"
                          removeApiMethod={removeClassification}
                          isShowMoreLess={false}
                          detailPage={false}
                          // entity={entity}
                        />
                      </Stack>
                    </Stack>
                  )
                )}
                {gtypeParams != "category" && <Divider />}{" "}
              </>
            )}
          {!isEmpty(gtypeParams) && gtypeParams == "category" && (
            <>
              {loading ? (
                <Stack direction="row" spacing={2} alignItems="center">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={200}
                    className="text-loader"
                  />
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={1000}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                !isEmpty(guid) &&
                gtypeParams == "category" && (
                  <Stack direction="row">
                    <Typography
                      lineHeight="26px"
                      flexBasis="12%"
                      fontWeight="800"
                      className="entity-attribute-label"
                    >
                      Terms:
                    </Typography>
                    <Stack
                      data-cy="termList"
                      direction="row"
                      flex="1"
                      justifyContent="flex-start"
                    >
                      <DialogShowMoreLess
                        value={data}
                        readOnly={false}
                        // setUpdateTable={setUpdateTable}
                        columnVal="terms"
                        colName="Term"
                        displayText="displayText"
                        removeApiMethod={removeTermorCategory}
                        isShowMoreLess={false}
                        detailPage={false}
                        relatedTerm={false}
                        // entity={entity}
                      />
                    </Stack>
                  </Stack>
                )
              )}
              <Divider />
            </>
          )}
          {!isEmpty(gtypeParams) && gtypeParams == "term" && (
            <>
              {loading ? (
                <Stack direction="row" spacing={2} alignItems="center">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={200}
                    className="text-loader"
                  />
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={1000}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                !isEmpty(guid) &&
                gtypeParams == "term" && (
                  <Stack direction="row">
                    <Typography
                      flexBasis="12%"
                      fontWeight="800"
                      className="entity-attribute-label"
                    >
                      Categories:
                    </Typography>
                    <Stack
                      data-cy="categoryList"
                      direction="row"
                      flex="1"
                      justifyContent="flex-start"
                    >
                      <DialogShowMoreLess
                        value={data}
                        readOnly={false}
                        // setUpdateTable={setUpdateTable}
                        columnVal="categories"
                        colName="Category"
                        displayText="displayText"
                        removeApiMethod={removeTermorCategory}
                        isShowMoreLess={false}
                        detailPage={false}
                        // entity={entity}
                      />
                    </Stack>
                  </Stack>
                )
              )}
            </>
          )}
          {!isEmpty(superTypes) && (
            <>
              {loading ? (
                <Stack direction="row" spacing={2} alignItems="center">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={200}
                    className="text-loader"
                  />
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={1000}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                <Stack direction="row">
                  <Typography
                    flexBasis="12%"
                    fontWeight="800"
                    className="entity-attribute-label"
                  >
                    Direct super-classifications:
                  </Typography>
                  <Stack
                    data-cy="direct-super-classifications"
                    direction="row"
                    flex="1"
                    justifyContent="flex-start"
                  >
                    <DialogShowMoreLess
                      value={data}
                      readOnly={true}
                      // setUpdateTable={setUpdateTable}
                      columnVal="superTypes"
                      colName="Classification"
                      displayText="superTypes"
                      // removeApiMethod={removeClassification}
                      isShowMoreLess={false}
                      detailPage={false}
                      // entity={entity}
                    />
                  </Stack>
                </Stack>
              )}
              <Divider />
            </>
          )}
          {!isEmpty(subTypes) && (
            <>
              {loading ? (
                <Stack direction="row" spacing={2} alignItems="center">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={200}
                    className="text-loader"
                  />
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={1000}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                <Stack direction="row">
                  <Typography
                    flexBasis="12%"
                    fontWeight="800"
                    className="entity-attribute-label"
                  >
                    Direct sub-classifications:
                  </Typography>
                  <Stack
                    data-cy="propagatedTagList"
                    direction="row"
                    flex="1"
                    justifyContent="flex-start"
                  >
                    <DialogShowMoreLess
                      value={data}
                      readOnly={true}
                      // setUpdateTable={setUpdateTable}
                      columnVal="subTypes"
                      colName="Classification"
                      displayText="subTypes"
                      // removeApiMethod={removeClassification}
                      isShowMoreLess={false}
                      detailPage={false}
                      // entity={entity}
                    />
                  </Stack>
                </Stack>
              )}
              <Divider />
            </>
          )}
          {!isEmpty(entityTypes) && (
            <>
              {loading ? (
                <Stack direction="row" spacing={2} alignItems="center">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={200}
                    className="text-loader"
                  />
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={1000}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                <Stack direction="row" gap="1rem">
                  <div className="detailpage-desc">
                    <Typography
                      lineHeight="26px"
                      flexBasis="12%"
                      fontWeight="800"
                      className="entity-attribute-label"
                    >
                      Entity-types:
                    </Typography>
                  </div>
                  <div className="flex-1" data-cy="entity-types">
                    <DialogShowMoreLess
                      value={data}
                      readOnly={true}
                      // setUpdateTable={setUpdateTable}
                      columnVal="subTypes"
                      colName="Classification"
                      displayText="subTypes"
                      // removeApiMethod={removeClassification}
                      isShowMoreLess={false}
                      detailPage={false}
                      // entity={entity}
                    />
                  </div>
                </Stack>
              )}
              <Divider />
            </>
          )}
          {attributeDefs != undefined && (
            <>
              <>
                {loading ? (
                  <Stack direction="row" spacing={2} alignItems="center">
                    <SkeletonLoader
                      count={1}
                      variant="text"
                      width={200}
                      className="text-loader"
                    />
                    <SkeletonLoader
                      count={1}
                      variant="text"
                      width={1000}
                      className="text-loader"
                    />
                  </Stack>
                ) : (
                  <Stack direction="row">
                    <Typography
                      lineHeight="26px"
                      flexBasis="12%"
                      fontWeight="800"
                      className="entity-attribute-label"
                    >
                      Attributes:
                    </Typography>
                    <Stack
                      data-cy="propagatedTagList"
                      direction="row"
                      flex="1"
                      justifyContent="flex-start"
                    >
                      <DialogShowMoreLess
                        value={data}
                        readOnly={false}
                        columnVal="attributeDefs"
                        colName="Attribute"
                        displayText="name"
                        isShowMoreLess={false}
                        detailPage={false}
                      />
                    </Stack>
                  </Stack>
                )}
              </>
            </>
          )}
        </StyledPaper>
      </Stack>

      {tagModal && (
        <ClassificationForm
          open={tagModal}
          onClose={handleCloseTagModal}
          setTagModal={setTagModal}
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
    </>
  );
};

export default DetailPageAttribute;
