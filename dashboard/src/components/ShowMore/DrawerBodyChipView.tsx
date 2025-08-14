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

import { EllipsisText } from "@components/commonComponents";
import CustomModal from "@components/Modal";
import { LightTooltip } from "@components/muiComponents";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import Paper from "@mui/material/Paper";
import InputBase from "@mui/material/InputBase";
import IconButton from "@mui/material/IconButton";
import Stack from "@mui/material/Stack";
import Chip from "@mui/material/Chip";
import Typography from "@mui/material/Typography";
import ClearIcon from "@mui/icons-material/Clear";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";
import { extractKeyValueFromEntity, isEmpty, serverError } from "@utils/Utils";
import { useRef, useState, ChangeEvent } from "react";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import { toast } from "react-toastify";
import SearchIcon from "@mui/icons-material/Search";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";
import { Link as MuiLink } from "@mui/material";
import { cloneDeep } from "@utils/Helper";

const CHIP_MAX_WIDTH = "200px";

interface DrawerBodyProps {
  data: Array<{ [key: string]: any }>;
  title: string;
  displayKey: string;
  currentEntity: any;
  removeApiMethod: any;
  removeTagsTitle: string;
  isDeleteIcon?: boolean;
}

const DrawerBodyChipView = ({
  data,
  currentEntity,
  title,
  removeApiMethod,
  displayKey,
  removeTagsTitle,
  isDeleteIcon
}: DrawerBodyProps) => {
  const location = useLocation();
  const toastId = useRef<any>(null);
  const navigate = useNavigate();
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");
  const dispatchApi = useAppDispatch();
  const { guid }: any = useParams();
  const { guid: entityGuid } = currentEntity || {};
  const [searchTerm, setSearchTerm] = useState<string>("");

  const [currentValue, setCurrentValue] = useState<{
    selectedValue: string;
    assetName: string;
  }>({
    selectedValue: "",
    assetName: ""
  });
  const { classificationData = {} }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { classificationDefs = {} } = classificationData || {};

  const [openTagRemoveModal, setOpenTagRemoveModal] = useState<boolean>(false);
  const [removeLoader, setRemoveLoader] = useState(false);

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

  const filteredData = !isEmpty(data)
    ? data.filter((tag: any) => {
        return (tag[displayKey] || tag)
          ?.toLowerCase()
          ?.includes(searchTerm?.toLowerCase());
      })
    : [];

  const handleRemove = async () => {
    try {
      setRemoveLoader(true);
      if (title == "Classifications") {
        await removeApiMethod(guid, currentValue.selectedValue);
      } else if (title == "Terms" || title == "Category") {
        let selectedTerm: any = !isEmpty(data)
          ? data?.find((obj: any) => {
              const typedObj = obj as {
                qualifiedName: string;
                displayText: string;
                termGuid: string;
              };
              if (
                (typedObj.qualifiedName || typedObj.displayText) ==
                currentValue.selectedValue
              ) {
                return typedObj;
              }
            })
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
      setRemoveLoader(false);

      toast.dismiss(toastId.current);
      toastId.current = toast.success(`${title}  was removed successfully`);
      handleCloseTagModal();

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
      setRemoveLoader(false);
      console.log(`Error occur while removing ${title}`, error);
      serverError(error, toastId);
    }
  };

  const checkSuperTypes = (classificationName: string) => {
    var tagObj = !isEmpty(classificationData.classificationDefs)
      ? classificationData.classificationDefs.find((obj: { name: string }) => {
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

  const getLabel = (label: string, optionalLabel?: string) => {
    if (title == "Classifications") {
      return checkSuperTypes(label);
    } else if (title == "Propagated Classifications") {
      return getTagParentList(label);
    } else {
      return label || optionalLabel;
    }
  };

  const getHref = (values: string, data: any | undefined) => {
    if (title == "Classifications" || title == "Propagated Classifications") {
      const searchParams = new URLSearchParams(location.search);
      searchParams.set("tag", values);

      return (
        <Link
          className="entity-name text-center text-blue text-decoration-none"
          to={{
            pathname: `/tag/tagAttribute/${values}`,
            search: `?${searchParams.toString()}`
          }}
          color={"primary"}
        >
          {getLabel(values, data)}
        </Link>
      );
    }

    if (title == "Terms") {
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
  return (
    <>
      <Paper
        sx={{
          width: "100%"
        }}
        variant="outlined"
        className="sidebar-searchbar"
      >
        <InputBase
          fullWidth
          sx={{ color: "rgba(0, 0, 0, 0.7)" }}
          placeholder="Search"
          inputProps={{ "aria-label": "search" }}
          value={searchTerm}
          onChange={(e: ChangeEvent<HTMLInputElement>) => {
            setSearchTerm(e.target.value);
          }}
        />

        {searchTerm?.length > 0 && (
          <IconButton
            type="submit"
            size="small"
            onClick={() => {
              setSearchTerm("");
            }}
            aria-label="search"
          >
            <ClearIcon fontSize="inherit" />
          </IconButton>
        )}

        <IconButton type="submit" size="small" aria-label="search">
          <SearchIcon fontSize="inherit" />
        </IconButton>
      </Paper>
      <Stack direction="column" alignItems="flex-start">
        <Stack
          data-cy="tag-list"
          direction="row"
          flex="1"
          justifyContent="flex-start"
        >
          <div className="tag-list" style={{ gap: "0.25rem" }}>
            {!isEmpty(filteredData) ? (
              filteredData?.map((obj: any, index: number) => {
                return (
                  <LightTooltip title={getLabel(obj[displayKey], obj)}>
                    <Chip
                      key={index}
                      className="chip-items"
                      color="primary"
                      component="a"
                      label={
                        <EllipsisText>
                          {getHref(obj[displayKey], obj)}
                        </EllipsisText>
                      }
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
              })
            ) : (
              <>
                <Typography>No Data Found</Typography>
              </>
            )}
          </div>
        </Stack>
      </Stack>
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
export default DrawerBodyChipView;
