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

import { LightTooltip } from "./muiComponents";
import { Chip, IconButton, Menu, MenuItem, Typography } from "@mui/material";
import { extractKeyValueFromEntity, isEmpty, serverError } from "@utils/Utils";
import { useRef, useState } from "react";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";
import CustomModal from "./Modal";
import { _delete } from "../api/apiMethods/apiMethod";
import moment from "moment-timezone";
import { toast } from "react-toastify";
import { EllipsisText } from "./commonComponents";
import { Link, useLocation, useParams } from "react-router-dom";
import AddCircleOutlineIcon from "@mui/icons-material/AddCircleOutline";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import AddTag from "@views/Classification/AddTag";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import AssignTerm from "@views/Glossary/AssignTerm";
import AssignCategory from "@views/Glossary/AssignCategory";
import AddTagAttributes from "@views/Classification/AddTagAttributes";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";

const CHIP_MAX_WIDTH = "200px";
const ITEM_HEIGHT = 48;

const DialogShowMoreLess = ({
  value,
  readOnly,
  setUpdateTable,
  columnVal,
  colName,
  displayText,
  optionalDisplayText,
  removeApiMethod,
  isShowMoreLess,
  detailPage,
  entity,
  relatedTerm
}: {
  value: any;
  readOnly?: boolean;
  setUpdateTable?: any;
  columnVal: string;
  colName: string;
  displayText: string;
  optionalDisplayText?: string;
  removeApiMethod?: any;
  isShowMoreLess: boolean;
  detailPage?: boolean;
  entity?: any;
  relatedTerm?: boolean;
}) => {
  const typedef: any = useAppSelector((state: any) => state.classification);
  const { classificationData }: any = typedef;
  const [openMenu, setOpenMenu] = useState<null | HTMLElement>(null);
  const [currentValue, setCurrentValue] = useState<{
    selectedValue: string;
    assetName: string;
  }>({
    selectedValue: "",
    assetName: ""
  });
  const open = Boolean(openMenu);
  const toastId: any = useRef(null);
  const { guid }: any = useParams();
  const dispatchApi = useAppDispatch();
  // const navigate = useNavigate();
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");
  const [openModal, setOpenModal] = useState<boolean>(false);
  const [tagModal, setTagModal] = useState<boolean>(false);
  const [termModal, setTermModal] = useState<boolean>(false);
  const [categoryModal, setCategoryModal] = useState<boolean>(false);
  const [attributeModal, setAttributeModal] = useState<boolean>(false);
  const [removeLoader, setRemoveLoader] = useState(false);

  const handleCloseTagModal = () => {
    setTagModal(false);
  };

  const handleCloseTermModal = () => {
    setTermModal(false);
  };
  const handleCloseCategoryModal = () => {
    setCategoryModal(false);
  };
  const handleCloseAttributeModal = () => {
    setAttributeModal(false);
  };
  const handleCloseModal = () => {
    setOpenModal(false);
  };

  const handleClick = (event: React.MouseEvent<HTMLElement>) => {
    setOpenMenu(event.currentTarget);
  };

  const handleClose = () => {
    setOpenMenu(null);
  };
  const handleDelete = (currentVal: string) => {
    let { name } = extractKeyValueFromEntity(detailPage ? entity : value);
    setCurrentValue({
      selectedValue: currentVal,
      assetName: name
    });
    setOpenMenu(null);
    setOpenModal(true);
  };

  const handleRemove = async () => {
    try {
      setRemoveLoader(true);
      if (colName == "Classification") {
        await removeApiMethod(
          detailPage ? entity.guid : value.guid,
          currentValue.selectedValue
        );
      } else if (colName == "Term" || colName == "Category") {
        let selectedTerm = value[columnVal].find(
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
        );
        if (isEmpty(guid) || detailPage) {
          await removeApiMethod(
            detailPage ? selectedTerm.guid : selectedTerm.termGuid,

            {
              guid: detailPage ? entity.guid : value.guid,
              relationshipGuid: detailPage
                ? selectedTerm.relationshipGuid
                : selectedTerm.relationGuid
            }
          );
        } else if (!detailPage && !isShowMoreLess) {
          let values = { ...value };
          let data;
          if (colName == "Term") {
            data = values?.[columnVal].filter(
              (obj: { displayText: string }) => {
                return obj.displayText != currentValue.selectedValue;
              }
            );

            values["terms"] = data;
          } else {
            data = values?.[columnVal].filter(
              (obj: { displayText: string }) => {
                return obj.displayText != currentValue.selectedValue;
              }
            );
            values["categories"] = data;
          }

          await removeApiMethod(
            guid,
            colName == "Term" ? "category" : "term",
            values
          );
        }
      }
      setOpenModal(false);
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `${colName} ${
          colName == "Term" ? "association" : currentValue.selectedValue
        } was removed successfully`
      );
      if (!isEmpty(guid)) {
        let params: any = { gtype: gType, guid: guid };
        dispatchApi(fetchGlossaryData());
        dispatchApi(fetchGlossaryDetails(params));
        dispatchApi(fetchDetailPageData(guid as string));
      }
      !isEmpty(setUpdateTable) && setUpdateTable(moment.now());
    } catch (error) {
      setOpenModal(false);
      console.log(`Error occur while removing ${colName}`, error);
      serverError(error, toastId);
    }
  };

  const checkSuperTypes = (classificationName: string) => {
    let tagObj = !isEmpty(classificationData.classificationDefs)
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

  const getLabel = (label: string, optionalLabel?: string) => {
    if (columnVal == "Classifications" || columnVal == "self") {
      return checkSuperTypes(label);
    } else {
      return label || optionalLabel;
    }
  };

  const getHref = (
    values: string,
    text: string | undefined,
    data: any | undefined
  ) => {
    if (colName == "Classification" || colName == "Propagated Classification") {
      let keys = Array.from(searchParams.keys());
      for (let i = 0; i < keys.length; i++) {
        // if (keys[i] != "searchType") {
        searchParams.delete(keys[i]);
        // }
      }
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
          {getLabel(values, text)}
        </Link>
      );
    }
    if (colName == "Term" || colName == "Category") {
      const { termGuid, categoryGuid }: any = data || {};
      const searchParams = new URLSearchParams(location.search);

      searchParams.set("gtype", colName == "Term" ? "term" : "category");
      searchParams.set("viewType", colName == "Term" ? "term" : "category");
      searchParams.set("fromView", "entity");

      let gTypeGuid =
        (colName == "Term" ? termGuid : categoryGuid) || data?.guid;

      return (
        <Link
          className="entity-name text-center text-blue text-decoration-none"
          to={{
            pathname: `/glossary/${gTypeGuid}`,
            search: `?${searchParams.toString()}`
          }}
          color={"primary"}
        >
          {getLabel(values, text)}
        </Link>
      );
    }
    return getLabel(values, text);
  };

  const assignTitle = () => {
    if (colName == "Classification") {
      return "Add Classification";
    } else if (colName == "Term") {
      return "Add Term";
    }
  };

  const removeTitle = () => {
    if (colName == "Classification") {
      return "Remove Classification Assignment";
    } else if (colName == "Term") {
      return "Remove Term Assignment";
    } else if (colName == "Category") {
      return "Remove Category Assignment";
    }
  };

  return (
    <>
      {value?.[columnVal]?.length > 0 ? (
        <div
          className="tag-list"
          style={{ flexWrap: isShowMoreLess ? "nowrap" : "wrap" }}
        >
          {isShowMoreLess && (
            <LightTooltip
              title={getLabel(
                value[columnVal][0][displayText],
                optionalDisplayText
              )}
            >
              <Chip
                color="primary"
                className="chip-items"
                label={
                  <EllipsisText>
                    {getHref(
                      value[columnVal][0][displayText],
                      optionalDisplayText,
                      (colName == "Term" || colName == "Category") &&
                        value[columnVal][0]
                    )}
                  </EllipsisText>
                }
                onDelete={
                  !isEmpty(removeApiMethod) &&
                  (colName !== "Classification" ||
                    value.guid === value[columnVal][0].entityGuid ||
                    (value.guid !== value[columnVal][0].entityGuid &&
                      value[columnVal][0].entityStatus === "DELETED"))
                    ? () => {
                        handleDelete(value[columnVal][0][displayText]);
                      }
                    : undefined
                }
                size="small"
                variant="outlined"
                sx={{
                  "& .MuiChip-label": {
                    display: "block",
                    overflow: "ellipsis",
                    maxWidth: "145px"
                  },

                  maxWidth: CHIP_MAX_WIDTH
                }}
                clickable
                data-cy="tagClick"
              />{" "}
            </LightTooltip>
          )}
          {!isShowMoreLess &&
            value?.[columnVal].map((obj: any, index: number) => {
              return (
                <LightTooltip
                  title={getLabel(obj[displayText] || obj, optionalDisplayText)}
                >
                  <Chip
                    key={index}
                    className="chip-items"
                    color="primary"
                    label={
                      <EllipsisText>
                        {getHref(
                          obj[displayText] || obj,
                          optionalDisplayText,
                          (colName == "Term" || colName == "Category") &&
                            value[columnVal][index]
                        )}
                      </EllipsisText>
                    }
                    onDelete={
                      !isEmpty(removeApiMethod) &&
                      (colName !== "Classification" ||
                        value.guid === obj.entityGuid ||
                        (value.guid !== obj.entityGuid &&
                          obj.entityStatus === "DELETED"))
                        ? () => {
                            handleDelete(obj[displayText] || obj);
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
          {value[columnVal].length > 1 && isShowMoreLess && (
            <LightTooltip>
              <IconButton
                data-cy="moreData"
                color="primary"
                size="small"
                onClick={handleClick}
                aria-controls={open ? "long-menu" : undefined}
                aria-expanded={open ? "true" : undefined}
                aria-haspopup="true"
              >
                <MoreHorizIcon />
              </IconButton>
            </LightTooltip>
          )}
          {!readOnly && (
            <LightTooltip title={assignTitle()}>
              <IconButton
                data-cy="addTag"
                color="primary"
                size="small"
                onClick={() => {
                  if (colName == "Classification") {
                    setTagModal(true);
                  } else if (colName == "Term") {
                    setTermModal(true);
                  } else if (colName == "Category") {
                    setCategoryModal(true);
                  } else if (colName == "Attribute") {
                    setAttributeModal(true);
                  }
                }}
              >
                <AddCircleOutlineIcon fontSize="small" />
              </IconButton>
            </LightTooltip>
          )}

          <Menu
            id="long-menu"
            MenuListProps={{
              "aria-labelledby": "long-button"
            }}
            anchorEl={openMenu}
            open={open}
            onClose={handleClose}
            PaperProps={{
              style: {
                maxHeight: ITEM_HEIGHT * 4.5
              }
            }}
          >
            {value?.[columnVal].map((obj: any, index: number) => {
              if (index > 0) {
                return (
                  <MenuItem key={obj[displayText]} onClick={handleClose}>
                    <LightTooltip
                      title={getLabel(obj[displayText], optionalDisplayText)}
                    >
                      <Chip
                        color="primary"
                        label={
                          <EllipsisText>
                            {getHref(
                              obj[displayText],
                              optionalDisplayText,
                              (colName == "Term" || colName == "Category") &&
                                value[columnVal][index]
                            )}
                          </EllipsisText>
                        }
                        className="chip-items"
                        onDelete={
                          !isEmpty(removeApiMethod) &&
                          (colName !== "Classification" ||
                            value.guid === obj.entityGuid ||
                            (value.guid !== obj.entityGuid &&
                              obj.entityStatus === "DELETED"))
                            ? () => {
                                handleDelete(obj[displayText] || obj);
                              }
                            : undefined
                        }
                        size="small"
                        variant="outlined"
                        clickable
                        sx={{
                          "& .MuiChip-label": {
                            display: "block",
                            overflow: "ellipsis",
                            maxWidth: "180px"
                          },
                          maxWidth: CHIP_MAX_WIDTH
                        }}
                      />
                    </LightTooltip>
                  </MenuItem>
                );
              }
            })}
          </Menu>
        </div>
      ) : (
        !readOnly && (
          <LightTooltip title={assignTitle()}>
            <IconButton
              data-cy="addTag"
              color="primary"
              size="small"
              onClick={() => {
                if (colName == "Classification") {
                  setTagModal(true);
                } else if (colName == "Term") {
                  setTermModal(true);
                } else if (colName == "Category") {
                  setCategoryModal(true);
                } else if (colName == "Attribute") {
                  setAttributeModal(true);
                }
              }}
            >
              <AddCircleOutlineIcon fontSize="small" />
            </IconButton>
          </LightTooltip>
        )
      )}
      {openModal && (
        <CustomModal
          open={openModal}
          onClose={handleCloseModal}
          title={relatedTerm ? "Confirmation" : (removeTitle() as string)}
          titleIcon={<ErrorRoundedIcon className="remove-modal-icon" />}
          button1Label="Cancel"
          button1Handler={handleCloseModal}
          button2Label="Remove"
          button2Handler={handleRemove}
          disableButton2={removeLoader}
        >
          {relatedTerm ? (
            <Typography fontSize={15}>
              Are you sure you want to remove term association
            </Typography>
          ) : (
            <Typography fontSize={15}>
              Remove: <b>{currentValue.selectedValue}</b> assignment from{" "}
              <b>{currentValue.assetName}</b> ?
            </Typography>
          )}
        </CustomModal>
      )}

      {tagModal && colName == "Classification" && (
        <AddTag
          open={tagModal}
          isAdd={true}
          entityData={value["entity"] != undefined ? value.entity : value}
          onClose={handleCloseTagModal}
          setUpdateTable={setUpdateTable}
          setRowSelection={undefined}
        />
      )}
      {termModal && colName == "Term" && !relatedTerm && (
        <AssignTerm
          open={termModal}
          // glossaryType={colName}
          onClose={handleCloseTermModal}
          data={value}
          relatedTerm={relatedTerm}
          updateTable={setUpdateTable}
        />
      )}

      {termModal && colName == "Term" && relatedTerm && (
        <AssignTerm
          open={termModal}
          // glossaryType={colName}
          onClose={handleCloseTermModal}
          data={value}
          relatedTerm={relatedTerm}
          updateTable={setUpdateTable}
          columnVal={columnVal}
        />
      )}

      {categoryModal && colName == "Category" && (
        <AssignCategory
          open={categoryModal}
          onClose={handleCloseCategoryModal}
          data={value}
          updateTable={setUpdateTable}
        />
      )}

      {attributeModal && colName == "Attribute" && (
        <AddTagAttributes
          open={attributeModal}
          onClose={handleCloseAttributeModal}
        />
      )}
    </>
  );
};

export default DialogShowMoreLess;
