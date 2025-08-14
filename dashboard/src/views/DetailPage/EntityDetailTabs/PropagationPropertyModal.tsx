// @ts-nocheck

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

import {
  getRelationshipData,
  saveRelationShip
} from "@api/apiMethods/lineageMethod";
import { TableLayout } from "@components/Table/TableLayout";
import {
  Stack,
  FormControlLabel,
  Switch,
  Typography,
  Radio,
  RadioGroup,
  FormControlLabelProps,
  styled,
  useRadioGroup,
  Divider,
  CircularProgress
} from "@mui/material";
import { useEffect, useMemo, useRef, useState } from "react";
import ArrowRightAltIcon from "@mui/icons-material/ArrowRightAlt";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import { LightTooltip } from "@components/muiComponents";
import { extractKeyValueFromEntity, isEmpty } from "@utils/Utils";
import CustomModal from "@components/Modal";
import { toast } from "react-toastify";
import { cloneDeep } from "@utils/Helper";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import { useAppDispatch } from "@hooks/reducerHook";

interface StyledFormControlLabelProps extends FormControlLabelProps {
  checked: boolean;
}

const StyledFormControlLabel = styled((props: StyledFormControlLabelProps) => (
  <FormControlLabel {...props} />
))(({ theme }) => ({
  variants: [
    {
      props: { checked: true },
      style: {
        ".MuiFormControlLabel-label": {
          color: theme.palette.primary.main
        }
      }
    }
  ]
}));

const PropagationPropertyModal = ({
  propagationModal,
  setPropagationModal,
  propagateDetails,
  lineageData,
  fetchGraph,
  initialQueryObj,
  refresh
}): any => {
  const relationshipDataRef = useRef([]);
  const dispatchApi = useAppDispatch();
  const navigate = useNavigate();
  const { relationshipId, edgeInfo, apiGuid } = propagateDetails;
  let fromEntity = lineageData.guidEntityMap[edgeInfo.fromEntityId];
  let toEntity = lineageData.guidEntityMap[edgeInfo.toEntityId];
  const location = useLocation();
  const { guid: entityId } = useParams();
  const searchParams = new URLSearchParams(location.search);
  const [checked, setChecked] = useState(false);
  const [propagationChecked, setPropagationChecked] = useState(false);
  const [loader, setLoader] = useState(false);
  const [loading, setLoading] = useState(false);
  const [selectedOption, setSelectedOption] = useState();
  const [relationshipData, setRelationshipData] = useState([]);

  useEffect(() => {
    fetchRelationShip();
  }, [relationshipId, fromEntity, toEntity]);

  const getPropagationFlow = (options) => {
    const { relationshipData, graphData } = options || {};
    const { propagateTags } = relationshipData;

    if (relationshipData.end1) {
      if (
        relationshipData.end1.guid == graphData.from.guid ||
        propagateTags == "BOTH" ||
        propagateTags == "NONE"
      ) {
        return propagateTags;
      } else {
        return propagateTags == "ONE_TO_TWO" ? "TWO_TO_ONE" : "ONE_TO_TWO";
      }
    } else {
      return propagateTags;
    }
  };

  function MyFormControlLabel(props: FormControlLabelProps) {
    const radioGroup = useRadioGroup();

    let checked = false;

    if (radioGroup) {
      checked = radioGroup.value === props.value;
    }
    return <StyledFormControlLabel checked={checked} {...props} />;
  }

  const fetchRelationShip = async () => {
    try {
      setLoader(true);
      const response = await getRelationshipData(
        {
          guid: relationshipId
        },
        { extendedInfo: true }
      );

      const { data } = response;
      const { relationship } = data;
      const selectedValue = getPropagationFlow({
        relationshipData: relationship,
        graphData: {
          id: relationshipId,
          from: fromEntity,
          to: toEntity
        }
      });
      apiGuid[relationship.guid] = data;
      relationshipDataRef.current = data; // Store in ref
      setRelationshipData(data);
      setSelectedOption(selectedValue);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      console.log("Error while fetching relationship data", error);
    }
  };

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setChecked(event.target.checked);
  };

  const { relationship = {} } = relationshipData || [];
  const { propagateTags, end1 = {} } = relationship || {};
  let isTwoToOne = false;
  if (edgeInfo.fromEntityId != end1.guid && propagateTags == "ONE_TO_TWO") {
    isTwoToOne = true;
  } else if (
    edgeInfo.fromEntityId == end1.guid &&
    propagateTags == "TWO_TO_ONE"
  ) {
    isTwoToOne = true;
  }

  const handleOptionsChange = (event) => {
    setSelectedOption(event.target.value);
  };

  const handlePropagationModal = () => {
    setPropagationModal(false);
  };

  const updateClassification = (classificationList, isChecked) => {
    return classificationList.map((val) => ({
      ...val,
      fromBlockClassification: isChecked
    }));
  };

  let blockedPropagatedClassifications =
    relationship.blockedPropagatedClassifications || [];
  let propagatedClassifications = relationship.propagatedClassifications || [];
  let blockpropagationData = [];
  let propagatedData = [];
  if (checked) {
    blockpropagationData = updateClassification(
      blockedPropagatedClassifications,
      true
    );
    propagatedData = updateClassification(propagatedClassifications, false);
  }
  const popagationData = [...blockpropagationData, ...propagatedData];

  const defaultColumns = useMemo(
    () => [
      {
        accessorKey: "typeName",
        cell: (info: any) => {
          let values: string[] = info.row.original;
          const { typeName }: any = values;
          let href: string = `/tag/tagAttribute/${typeName}`;
          return (
            <Stack>
              <Typography fontWeight="600">
                <LightTooltip title={typeName}>
                  <Link
                    className="entity-name text-blue text-decoration-none"
                    to={{
                      pathname: href
                    }}
                    style={{ width: "unset !important", whiteSpace: "nowrap" }}
                    color={"primary"}
                  >
                    {typeName}
                  </Link>
                </LightTooltip>
              </Typography>
            </Stack>
          );
        },
        header: "Classification",
        enableSorting: false
      },
      {
        accessorKey: "entityGuid",
        cell: (info: any) => {
          let values: string[] = info.row.original;
          const { entityGuid }: any = values;
          let entityObj = apiGuid[relationshipId].referredEntities[entityGuid];

          let entityName: any = entityGuid;
          if (entityObj) {
            const { name }: { name: string; found: boolean; key: any } =
              extractKeyValueFromEntity(entityObj);
            entityName = name + " (" + entityObj.typeName + ")";
          }
          let href: string = `/detailPage/${entityGuid}`;
          searchParams.set("tabActive", "properties");
          return (
            <Stack>
              <Typography fontWeight="600">
                <LightTooltip title={entityName}>
                  <Link
                    className="entity-name   text-blue text-decoration-none"
                    to={{
                      pathname: href,
                      search: searchParams.toString()
                    }}
                    style={{ width: "unset !important", whiteSpace: "nowrap" }}
                    color={"primary"}
                  >
                    {entityName}
                  </Link>
                </LightTooltip>
              </Typography>
            </Stack>
          );
        },
        header: "Entity Name",
        enableSorting: false
      },
      {
        accessorKey: "Action",
        cell: ({ row, value, column: { id }, updateData }) => {
          let values = row.original;
          const { fromBlockClassification, typeName } = values;

          return (
            <input
              type="checkbox"
              checked={fromBlockClassification}
              onChange={(e) => {
                const relationshipRefData = cloneDeep(
                  relationshipDataRef.current
                );
                const { relationship = {} } = relationshipRefData || [];
                let blockedPropagatedClassifications =
                  relationship.blockedPropagatedClassifications || [];
                let propagatedClassifications =
                  relationship.propagatedClassifications || [];
                let blockpropagationData = [];
                let propagatedData = [];

                if (e.target.checked) {
                  propagatedClassifications = propagatedClassifications.filter(
                    (val) => {
                      if (
                        val.entityGuid === values.entityGuid &&
                        val.typeName === values.typeName
                      ) {
                        blockpropagationData.push(val);
                        return false;
                      }
                      return true;
                    }
                  );
                } else {
                  blockedPropagatedClassifications =
                    blockedPropagatedClassifications.filter((val) => {
                      if (
                        val.entityGuid === values.entityGuid &&
                        val.typeName === values.typeName
                      ) {
                        propagatedData.push(val);
                        return false;
                      }
                      return true;
                    });
                }
                setRelationshipData({
                  ...relationshipRefData,
                  relationship: {
                    ...relationship,
                    blockedPropagatedClassifications: [
                      ...blockedPropagatedClassifications,
                      ...blockpropagationData
                    ],
                    propagatedClassifications: [
                      ...propagatedClassifications,
                      ...propagatedData
                    ]
                  }
                });
                relationshipDataRef.current = {
                  ...relationshipRefData,
                  relationship: {
                    ...relationship,
                    blockedPropagatedClassifications: [
                      ...blockedPropagatedClassifications,
                      ...blockpropagationData
                    ],
                    propagatedClassifications: [
                      ...propagatedClassifications,
                      ...propagatedData
                    ]
                  }
                };
              }}
            />
          );
        },

        header: "Block Propagation",
        enableSorting: false,
        width: "10%"
      }
    ],
    []
  );

  const onSubmit = async () => {
    let relationshipProp = {};

    if (checked) {
      relationshipProp = {
        blockedPropagatedClassifications: blockedPropagatedClassifications,
        propagatedClassifications: propagatedClassifications
      };
    } else {
      relationshipProp = {
        propagateTags: getPropagationFlow({
          relationshipData: Object.assign(
            {},
            apiGuid?.[relationshipId]?.relationship,
            { propagateTags: selectedOption }
          ),
          graphData: { from: { guid: edgeInfo.fromEntityId } }
        })
      };
    }

    let data = Object.assign(
      {},
      apiGuid[relationshipId].relationship,
      relationshipProp
    );
    try {
      setLoading(true);
      await saveRelationShip(data);
      setLoading(false);
      toast.success("Propagation flow updated succesfully.");
      setPropagationModal(false);
      dispatchApi(fetchDetailPageData(entityId as string));

      // const searchParams = new URLSearchParams();
      // searchParams.set("tabActive", "lineage");
      // navigate(
      //   {
      //     pathname: `/detailPage/${entityId}`,
      //     search: searchParams.toString()
      //   },
      //   { replace: true }
      // );
      fetchGraph({
        queryParam: initialQueryObj,
        legends: false
      });
      refresh();
    } catch (error) {
      setLoading(false);
      console.log("Error while fetching relationship data", error);
    }
  };

  return (
    <>
      <CustomModal
        open={propagationModal}
        maxWidth="md"
        onClose={handlePropagationModal}
        title={"Classification Propagation Control"}
        button1Label="Cancel"
        button1Handler={handlePropagationModal}
        button2Label="Update"
        button2Handler={() => onSubmit()}
        disableButton2={loading}
      >
        <Stack gap={2}>
          <Stack direction="row" alignItems="center" gap="0.5rem">
            <Stack>
              <Typography fontWeight="400">
                Enable/Disable Propagation
              </Typography>
            </Stack>
            <Stack>
              <FormControlLabel
                sx={{ margin: 0 }}
                control={
                  <Switch
                    size="small"
                    checked={checked}
                    onChange={handleChange}
                    sx={{
                      "& .MuiSwitch-switchBase.Mui-checked": {
                        color: "blue"
                      },
                      "& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track":
                        {
                          backgroundColor: "blue"
                        },
                      "& .MuiSwitch-switchBase": {
                        color: "blue"
                      },
                      "& .MuiSwitch-switchBase + .MuiSwitch-track": {
                        backgroundColor: "blue"
                      }
                    }}
                  />
                }
                label={undefined}
              />
            </Stack>
            <Stack>
              <Typography fontWeight="400">
                Select Classifications to Block Propagation
              </Typography>
            </Stack>
          </Stack>
          <Divider />

          {checked ? (
            <>
              <TableLayout
                data={popagationData}
                columns={defaultColumns}
                emptyText="No Records found!"
                isFetching={loader}
                columnVisibility={false}
                clientSideSorting={true}
                columnSort={false}
                showPagination={false}
                showRowSelection={false}
                tableFilters={false}
              />
            </>
          ) : (
            <Stack>
              {loader ? (
                <Stack
                  direction="row"
                  alignItems={"center"}
                  justifyContent={"center"}
                >
                  <CircularProgress />
                </Stack>
              ) : (
                <Stack>
                  {fromEntity && toEntity && (
                    <Stack direction="row" gap="0.5rem" alignItems="center">
                      <Typography fontSize="18px" fontWeight="400">
                        {fromEntity.displayText}
                      </Typography>

                      <ArrowRightAltIcon color="success" />

                      <Typography fontSize="18px" fontWeight="400">
                        {toEntity.displayText}
                      </Typography>
                    </Stack>
                  )}

                  <RadioGroup
                    sx={{ paddingTop: "0.75rem", paddingLeft: "2rem" }}
                    name="propagationOptions"
                    data-cy="propagationOptions"
                    value={selectedOption}
                    onChange={handleOptionsChange}
                  >
                    <MyFormControlLabel
                      label={
                        <Stack direction="row" gap="0.5rem" alignItems="center">
                          <Typography fontSize="14px" fontWeight="400">
                            {fromEntity.typeName}
                          </Typography>
                          <ArrowRightAltIcon color="success" />
                          <Typography fontSize="14px" fontWeight="400">
                            {toEntity.typeName}
                          </Typography>
                        </Stack>
                      }
                      value="ONE_TO_TWO"
                      name="propagateRelation"
                      control={<Radio size="small" />}
                    />

                    {isTwoToOne && (
                      <MyFormControlLabel
                        name="propagateRelation"
                        value="TWO_TO_ONE"
                        label={
                          <Stack
                            direction="row"
                            gap="0.5rem"
                            alignItems="center"
                          >
                            <Typography fontSize="14px" fontWeight="400">
                              {fromEntity.typeName}
                            </Typography>
                            <ArrowRightAltIcon color="success" />
                            <Typography fontSize="14px" fontWeight="400">
                              {toEntity.typeName}
                            </Typography>
                          </Stack>
                        }
                        control={<Radio size="small" />}
                      />
                    )}
                    {propagateTags == "BOTH" && (
                      <MyFormControlLabel
                        name="propagateRelation"
                        value="Both"
                        label={
                          <Stack
                            direction="row"
                            gap="0.5rem"
                            alignItems="center"
                          >
                            <Typography fontSize="14px" fontWeight="400">
                              {fromEntity.typeName}
                            </Typography>
                            <ArrowRightAltIcon color="success" />
                            <Typography fontSize="14px" fontWeight="400">
                              {toEntity.typeName}
                            </Typography>
                          </Stack>
                        }
                        control={<Radio size="small" />}
                      />
                    )}
                    <MyFormControlLabel
                      name="propagateRelation"
                      value="NONE"
                      label="None"
                      control={<Radio size="small" />}
                    />
                  </RadioGroup>
                </Stack>
              )}
            </Stack>
          )}
        </Stack>
      </CustomModal>
    </>
  );
};

export default PropagationPropertyModal;
