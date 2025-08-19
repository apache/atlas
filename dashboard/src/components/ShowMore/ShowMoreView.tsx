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

import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import MuiLink from "@mui/material/Link";
import { LightTooltip } from "../muiComponents";
import { useRef, useState } from "react";
import { EllipsisText } from "../commonComponents";
import { extractKeyValueFromEntity, isEmpty, serverError } from "@utils/Utils";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import CustomModal from "../Modal";
import { toast } from "react-toastify";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";
import ShowMoreDrawer from "./ShowMoreDrawer";
import { openDrawer } from "@redux/slice/drawerSlice";
import { cloneDeep } from "@utils/Helper";

const CHIP_MAX_WIDTH = "200px";

type TagDisplayWithDrawerProps = {
  data: any;
  maxVisible?: number;
  title?: string;
  displayKey: string;
  removeApiMethod?: any;
  currentEntity?: any;
  removeTagsTitle?: string;
  isEditView: boolean;
  isDeleteIcon?: boolean;
};

interface Tag {
  typeName: string;
  text?: string;
  [key: string]: any;
}

const ShowMoreView = ({
  data,
  maxVisible = 4,
  title,
  displayKey,
  removeApiMethod,
  currentEntity,
  removeTagsTitle,
  isEditView,
  isDeleteIcon
}: TagDisplayWithDrawerProps & { id: string }) => {
  const location = useLocation();
  const { guid } = useParams();
  const toastId = useRef<any>(null);
  const navigate = useNavigate();
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");
  const dispatchApi = useAppDispatch();

  const { classificationData = {} }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { isOpen, activeId } = useAppSelector(
    (state: any) => state.drawerState
  );

  const { classificationDefs = {} } = classificationData || {};
  const { guid: entityGuid } = currentEntity || {};

  const [currentValue, setCurrentValue] = useState<{
    selectedValue: string;
    assetName: string;
  }>({
    selectedValue: "",
    assetName: ""
  });
  const [openTagRemoveModal, setOpenTagRemoveModal] = useState<boolean>(false);

  const [removeLoader, setRemoveLoader] = useState(false);

  const [activeData, setActiveData] = useState<any>(data);

  const hasMore = !isEmpty(data) ? data.length > maxVisible : false;
  const visibleTags = hasMore ? data.slice(0, maxVisible) : data;

  const handleCloseTagModal = () => {
    setOpenTagRemoveModal(false);
  };

  const handleDelete = (currentVal: string) => {
    let { name } = extractKeyValueFromEntity(currentEntity);
    setCurrentValue({
      selectedValue: currentVal,
      assetName: name
    });
    setOpenTagRemoveModal(true);
  };

  const handleRemove = async () => {
    try {
      setRemoveLoader(true);
      if (title == "Classifications") {
        await removeApiMethod(guid, currentValue.selectedValue);
      } else if (title == "Terms" || title == "Category") {
        let selectedTerm = !isEmpty(data)
          ? data?.find(
              (obj: {
                qualifiedName: string;
                displayText: string;
                termGuid: string;
              }) => {
                if (
                  (obj.qualifiedName || obj.displayText) ==
                  currentValue.selectedValue
                ) {
                  return obj;
                }
              }
            )
          : {};

        if (isEmpty(gType)) {
          await removeApiMethod(selectedTerm.guid, {
            guid: entityGuid,
            relationshipGuid: selectedTerm.relationshipGuid
          });
        } else if (!isEmpty(gType)) {
          let values = cloneDeep(currentEntity);
          let glossaryData;
          if (title == "Terms") {
            glossaryData = values?.["terms"].filter(
              (obj: { displayText: string }) => {
                return obj.displayText != currentValue.selectedValue;
              }
            );

            values["terms"] = glossaryData;
          } else {
            glossaryData = values?.["categories"].filter(
              (obj: { displayText: string }) => {
                return obj.displayText != currentValue.selectedValue;
              }
            );
            values["categories"] = glossaryData;
          }

          await removeApiMethod(
            guid,
            title == "Terms" ? "category" : "term",
            values
          );
        }
      }
      toast.dismiss(toastId.current);
      toastId.current = toast.success(`${title}  was removed successfully`);
      dispatchApi(fetchDetailPageData(guid as string));

      if (!isEmpty(guid)) {
        dispatchApi(fetchDetailPageData(guid as string));

        if (!isEmpty(gType)) {
          const params = { gtype: gType, guid };
          dispatchApi(fetchGlossaryData());
          dispatchApi(fetchGlossaryDetails(params));
        }
      }
    } catch (error) {
      console.log(`Error occur while removing ${title}`, error);
      serverError(error, toastId);
    }
  };

  const checkSuperTypes = (classificationName: string) => {
    var tagObj = !isEmpty(classificationDefs)
      ? classificationDefs.find((obj: { name: string }) => {
          return obj.name == classificationName;
        })
      : {};

    return !isEmpty(tagObj?.superTypes)
      ? tagObj.superTypes.length > 1
        ? `${classificationName}@(${tagObj.superTypes.join(", ")})`
        : `${classificationName}@${tagObj.superTypes.join()}`
      : classificationName;
  };

  const getTagParentList = (name: string) => {
    let tagObj = !isEmpty(classificationDefs)
        ? classificationDefs?.find((obj: { name: string }) => {
            return obj.name == name;
          })
        : {},
      tagParents = tagObj ? tagObj?.["superTypes"] : null,
      parentName = name;
    if (tagParents && tagParents.length) {
      parentName +=
        tagParents.length > 1
          ? "@(" + tagParents.join() + ")"
          : "@" + tagParents.join();
    }
    return parentName;
  };
  const getLabel = (label: string, optionalLabel?: any) => {
    if (title == "Classifications") {
      return checkSuperTypes(label);
    } else if (title == "Propagated Classifications") {
      return getTagParentList(label);
    } else {
      return label || optionalLabel;
    }
  };

  const getHref = (values: string, data: any | undefined) => {
    if (
      title == "Classifications" ||
      title == "Propagated Classifications" ||
      title == "Super Classifications" ||
      title == "Sub Classifications"
    ) {
      return (
        <Link
          className="entity-name text-center text-blue text-decoration-none"
          to={{
            pathname: `/tag/tagAttribute/${values || data}`
          }}
          color={"primary"}
        >
          {getLabel(values, data)}
        </Link>
      );
    }

    if (title == "Terms" || title == "Category") {
      const { termGuid, categoryGuid }: any = data || {};
      const searchParams = new URLSearchParams(location.search);

      searchParams.set("gtype", title == "Terms" ? "term" : "category");
      searchParams.set("viewType", title == "Terms" ? "term" : "category");
      searchParams.set("fromView", "entity");

      let gTypeGuid =
        (title == "Terms" ? termGuid : categoryGuid) || data?.guid;

      return (
        <Link
          className="entity-name text-center text-blue text-decoration-none"
          to={{
            pathname: `/glossary/${gTypeGuid}`,
            search: `?${searchParams.toString()}`
          }}
          color={"primary"}
        >
          {getLabel(values, data)}
        </Link>
      );
    }
    return getLabel(values, data);
  };

  const handleSeeAllClick = (data: any, id: string) => {
    setActiveData(data);
    dispatchApi(openDrawer(id));
  };

  return (
    <>
      <div className="tag-list" style={{ alignItems: "baseline" }}>
        {!isEmpty(visibleTags) &&
          visibleTags.map((obj: Tag, index: number) => {
            return (
              <LightTooltip title={getLabel(obj[displayKey], obj)} key={index}>
                <Chip
                  color="primary"
                  className="chip-items"
                  label={
                    <EllipsisText>{getHref(obj[displayKey], obj)}</EllipsisText>
                  }
                  deleteIcon={
                    isDeleteIcon && obj.count > 1 ? (
                      <MuiLink component="text" underline="hover">
                        <Typography
                          lineHeight="24px"
                          fontWeight="500"
                        >{`(${obj.count})`}</Typography>
                      </MuiLink>
                    ) : undefined
                  }
                  component="a"
                  onDelete={
                    !isEmpty(removeApiMethod) && !isDeleteIcon
                      ? () => {
                          handleDelete(obj[displayKey] || obj);
                        }
                      : isDeleteIcon && obj.count > 1
                      ? () => {
                          const searchParams = new URLSearchParams();
                          searchParams.set("tabActive", "classification");
                          searchParams.set("filter", obj.typeName);
                          navigate({
                            pathname: `/detailPage/${guid}`,
                            search: searchParams.toString()
                          });
                        }
                      : undefined
                  }
                  size="small"
                  variant="outlined"
                  sx={{
                    "& .MuiChip-label": {
                      display: "block",
                      overflow: "ellipsis",
                      maxWidth: "180px"
                    },
                    maxWidth: CHIP_MAX_WIDTH
                  }}
                  clickable
                />
              </LightTooltip>
            );
          })}
        {hasMore && (
          <>
            <Typography
              variant="body2"
              color="textSecondary"
              sx={{ margin: "0" }}
            >
              ...
            </Typography>
            <MuiLink
              component="button"
              underline="hover"
              data-cy="assignTag"
              className="cursor-pointer"
              onClick={() => handleSeeAllClick(data, title || "")}
            >
              <Typography fontWeight={600}>See All</Typography>
            </MuiLink>
          </>
        )}
      </div>

      {isOpen && activeId === title && (
        <ShowMoreDrawer
          data={activeData}
          displayKey={displayKey}
          currentEntity={currentEntity}
          removeApiMethod={removeApiMethod}
          removeTagsTitle={removeTagsTitle || ""}
          title={title || ""}
          isEditView={isEditView}
          isDeleteIcon={isDeleteIcon}
        />
      )}

      {openTagRemoveModal && (
        <CustomModal
          open={openTagRemoveModal}
          onClose={handleCloseTagModal}
          title={removeTagsTitle as string}
          titleIcon={<ErrorRoundedIcon className="remove-modal-icon" />}
          button1Label="Cancel"
          button1Handler={handleCloseTagModal}
          button2Label="Remove"
          button2Handler={handleRemove}
          disableButton2={removeLoader}
        >
          <Typography fontSize={14}>
            Remove:{" "}
            <Typography
              component="span"
              display="inline"
              sx={{ fontWeight: 600 }}
            >
              {currentValue.selectedValue}
            </Typography>{" "}
            assignment from{" "}
            <Typography
              component="span"
              display="inline"
              sx={{ fontWeight: 600 }}
            >
              {!isEmpty(currentEntity)
                ? `${currentValue.assetName} ${
                    !isEmpty(currentEntity?.typeName)
                      ? `(${currentEntity.typeName})`
                      : ""
                  }`
                : ""}
            </Typography>{" "}
            ?
          </Typography>
        </CustomModal>
      )}
    </>
  );
};

export default ShowMoreView;
