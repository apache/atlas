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

import { useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
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
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import AddCircleOutlineIcon from "@mui/icons-material/AddCircleOutline";
import { StyledPaper } from "@utils/Muiutils";
import {
  extractKeyValueFromEntity,
  isEmpty,
  sanitizeHtmlContent
} from "@utils/Utils";
import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import {
  assignGlossaryType,
  assignTermstoEntites,
  removeTermorCategory
} from "@api/apiMethods/glossaryApiMethod";
import ClassificationForm from "@views/Classification/ClassificationForm";
import AddUpdateTermForm from "@views/Glossary/AddUpdateTermForm";
import AddUpdateCategoryForm from "@views/Glossary/AddUpdateCategoryForm";
import ShowMoreView from "@components/ShowMore/ShowMoreView";
import AddTag from "@views/Classification/AddTag";
import AddTagAttributes from "@views/Classification/AddTagAttributes";
import ShowMoreText from "@components/ShowMore/ShowMoreText";
import AddUpdateGlossaryForm from "@views/Glossary/AddUpdateGlossaryForm";
import AssignGlossaryItem from "@views/Glossary/AssignGlossaryItem";

const initialModalState = {
  tag: false,
  editTerm: false,
  editCategory: false,
  glossary: false,
  addTag: false,
  attribute: false,
  addTerm: false,
  category: false
};

const Loader = () => (
  <Stack direction="column" spacing={2} alignItems="flex-start">
    <SkeletonLoader
      count={1}
      variant="text"
      width={300}
      className="text-loader"
    />
  </Stack>
);

const Section = ({
  title,
  tooltip,
  onAdd,
  dataKey,
  showMoreProps,
  loading,
  children,
  data
}: any) => (
  <Stack direction="column" alignItems="flex-start" gap="0.5rem">
    <Stack direction="row" alignItems="center" gap="0.75rem">
      <Typography
        fontWeight="600"
        className="entity-attribute-label"
        fontSize="16px"
      >
        {title}
      </Typography>
      {tooltip && (
        <LightTooltip title={tooltip}>
          <IconButton
            component="label"
            size="small"
            color="primary"
            onClick={onAdd}
          >
            <AddCircleOutlineIcon className="mr-0" fontSize="small" />
          </IconButton>
        </LightTooltip>
      )}
    </Stack>
    <Stack direction="row" flex="1" justifyContent="flex-start">
      {loading ? (
        <Loader />
      ) : (
        children || (
          <ShowMoreView
            data={data?.[dataKey] || []}
            {...showMoreProps}
            currentEntity={data}
            isEditView={false}
          />
        )
      )}
    </Stack>
  </Stack>
);

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
  const [modals, setModals] = useState(initialModalState);

  const handleModal = (modal: keyof typeof initialModalState, open: boolean) =>
    setModals((prev) => ({ ...prev, [modal]: open }));

  const handleChange = (
    _event: React.MouseEvent<HTMLElement>,
    newAlignment: string
  ) => {
    setAlignment(newAlignment);
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
                      handleModal("tag", true);
                    } else if (!isEmpty(guid)) {
                      switch (gtypeParams) {
                        case "glossary":
                          handleModal("glossary", true);
                          break;
                        case "term":
                          handleModal("editTerm", true);
                          break;
                        case "category":
                          handleModal("editCategory", true);
                          break;
                        default:
                          break;
                      }
                    }
                  }}
                  data-cy="editButton"
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
            {shortDescription !== undefined && (
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
                  {shortDescription !== undefined ? `Long` : ""} Description
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
                  {alignment === "formatted" ? (
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
            {!isEmpty(gtypeParams) && gtypeParams == "term" && (
              <Section
                title="Classifications"
                tooltip="Add Classifications"
                onAdd={() => handleModal("addTag", true)}
                dataKey="classifications"
                showMoreProps={{
                  maxVisible: 4,
                  title: "Classifications",
                  displayKey: "typeName",
                  removeApiMethod: removeClassification,
                  removeTagsTitle: "Remove Classification Assignment",
                  id: "Classifications"
                }}
                loading={loading}
                data={data}
              />
            )}
            {!isEmpty(gtypeParams) && gtypeParams === "category" && (
              <Section
                title="Terms"
                tooltip="Add Term"
                onAdd={() => handleModal("addTerm", true)}
                dataKey="terms"
                showMoreProps={{
                  maxVisible: 4,
                  title: "Terms",
                  displayKey: "displayText",
                  removeApiMethod: removeTermorCategory,
                  removeTagsTitle: "Remove Term Assignment",
                  id: "Terms"
                }}
                loading={loading}
                data={data}
              />
            )}
            {!isEmpty(gtypeParams) && gtypeParams === "term" && (
              <Section
                title="Categories"
                tooltip="Add Categories"
                onAdd={() => handleModal("category", true)}
                dataKey="categories"
                showMoreProps={{
                  maxVisible: 4,
                  title: "Category",
                  displayKey: "displayText",
                  removeApiMethod: removeTermorCategory,
                  removeTagsTitle: "Remove Category Assignment",
                  id: "Category"
                }}
                loading={loading}
                data={data}
              />
            )}
            {!isEmpty(superTypes) && (
              <Section
                title="Direct super-classifications"
                dataKey="superTypes"
                showMoreProps={{
                  maxVisible: 4,
                  title: "Super Classifications",
                  displayKey: "",
                  id: "Super Classifications"
                }}
                loading={loading}
                data={data}
              />
            )}
            {!isEmpty(subTypes) && (
              <Section
                title="Direct sub-classifications"
                dataKey="subTypes"
                showMoreProps={{
                  maxVisible: 4,
                  title: "Sub Classifications",
                  displayKey: "",
                  id: "Sub Classifications"
                }}
                loading={loading}
                data={data}
              />
            )}
            {attributeDefs !== undefined && (
              <Section
                title="Attributes:"
                tooltip="Add Attributes"
                onAdd={() => handleModal("attribute", true)}
                dataKey="attributeDefs"
                showMoreProps={{
                  maxVisible: 4,
                  title: "Attributes",
                  displayKey: "name",
                  id: "Attributes"
                }}
                loading={loading}
                data={data}
              />
            )}
          </div>
        </StyledPaper>
      </Stack>

      {modals.tag && (
        <ClassificationForm
          open={modals.tag}
          onClose={() => handleModal("tag", false)}
          setTagModal={undefined}
        />
      )}
      {modals.addTag && (
        <AddTag
          open={modals.addTag}
          isAdd={true}
          entityData={data}
          onClose={() => handleModal("addTag", false)}
          setUpdateTable={undefined}
          setRowSelection={undefined}
        />
      )}
      {modals.editTerm && (
        <AddUpdateTermForm
          open={modals.editTerm}
          isAdd={false}
          onClose={() => handleModal("editTerm", false)}
          node={undefined}
          dataObj={data}
        />
      )}
      {modals.editCategory && (
        <AddUpdateCategoryForm
          open={modals.editCategory}
          isAdd={false}
          onClose={() => handleModal("editCategory", false)}
          node={undefined}
          dataObj={data}
        />
      )}
      {modals.attribute && (
        <AddTagAttributes
          open={modals.attribute}
          onClose={() => handleModal("attribute", false)}
        />
      )}

      {modals.category && (
        <AssignGlossaryItem
          open={modals.category}
          onClose={() => handleModal("category", false)}
          data={data || {}}
          updateTable={undefined}
          relatedItem={false}
          itemType="category"
          dataKey="categories"
          assignApiMethod={assignGlossaryType}
          treeLabel="Category"
        />
      )}

      {modals.addTerm && (
        <AssignGlossaryItem
          open={modals.addTerm}
          onClose={() => handleModal("addTerm", false)}
          data={data || {}}
          updateTable={undefined}
          relatedItem={undefined}
          itemType="term"
          dataKey="terms"
          assignApiMethod={assignTermstoEntites}
          treeLabel="Term"
        />
      )}

      {modals.glossary && (
        <AddUpdateGlossaryForm
          open={modals.glossary}
          isAdd={false}
          onClose={() => handleModal("glossary", false)}
          node={data || {}}
        />
      )}
    </>
  );
};

export default DetailPageAttribute;
