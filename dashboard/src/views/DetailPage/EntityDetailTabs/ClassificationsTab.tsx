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

import { useMemo, useRef, useState } from "react";
import {
  Autocomplete,
  Chip,
  FormControlLabel,
  FormGroup,
  Grid,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import { TableLayout } from "@components/Table/TableLayout";
import { EntityDetailTabProps } from "@models/entityDetailType";
import {
  customSortBy,
  extractKeyValueFromEntity,
  getBoolean,
  isEmpty,
  serverError
} from "@utils/Utils";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { CustomButton, LightTooltip } from "@components/muiComponents";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import DeleteOutlinedIcon from "@mui/icons-material/DeleteOutlined";
import { isEntityPurged } from "@utils/Enum";
import CustomModal from "@components/Modal";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";
import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import { toast } from "react-toastify";
import AttributeTable from "../AttributeTable";
import AddTag from "@views/Classification/AddTag";
import moment from "moment";
import { useAppDispatch } from "@hooks/reducerHook";
import { AntSwitch } from "@utils/Muiutils";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";

const ClassificationsTab: React.FC<EntityDetailTabProps> = ({
  entity,
  loading,
  tags
}) => {
  const { classifications = {} } = entity || {};
  const toastId: any = useRef(null);
  const { guid }: any = useParams();
  const dispatchApi = useAppDispatch();
  const [searchParams, setSearchParams] = useSearchParams();
  const [currentValue, setCurrentValue] = useState<{
    selectedValue: string;
    assetName: string;
  }>({
    selectedValue: "",
    assetName: ""
  });
  const [classificationName, setClassificationName] = useState<string | null>(
    "All"
  );
  const [checked, setChecked] = useState(
    !isEmpty(searchParams.get("showPC"))
      ? getBoolean(searchParams.get("showPC") as string)
      : true
  );

  const [openModal, setOpenModal] = useState<boolean>(false);
  const [tagModal, setTagModal] = useState<boolean>(false);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [rowdata, setRowData] = useState();

  let newClassifications = structuredClone(classifications);
  let options = !isEmpty(newClassifications)
    ? (checked ? newClassifications : tags?.self)?.map(
        (obj: { typeName: string }) => obj.typeName
      )
    : [];

  const classificationData = [...["All"], ...options].sort();
  let data = checked
    ? !isEmpty(newClassifications)
      ? customSortBy(newClassifications, ["typeName"])
      : []
    : !isEmpty(tags?.self)
    ? customSortBy(tags?.self, ["typeName"])
    : [];
  let tableData = !isEmpty(classificationName)
    ? classificationName == "All"
      ? data
      : data.filter(
          (obj: { typeName: string }) => obj.typeName == classificationName
        )
    : data;

  const handleCloseTagModal = () => {
    setTagModal(false);
  };

  const handleCloseModal = () => {
    setOpenModal(false);
  };

  const handleChange = (newValue: string) => {
    setClassificationName(newValue as string);
  };
  const handleSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    event.stopPropagation();
    const newParams = new URLSearchParams(searchParams.toString());
    newParams.append("tabActive", "classification");
    newParams.append("showPC", event.target.checked ? "true" : "false");
    newParams.append("filter", "all");
    setChecked(event.target.checked);
    setSearchParams(newParams);
    handleChange("All");
  };

  const handleRemove = async () => {
    try {
      await removeClassification(guid, currentValue.selectedValue);
      if (!isEmpty(guid)) {
        dispatchApi(fetchDetailPageData(guid as string));
      }
      setSearchParams({
        tabActive: "classification",
        showPC: "true",
        filter: "all"
      });
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `Classification ${currentValue.selectedValue} was removed successfully`
      );
      setOpenModal(false);
    } catch (error) {
      console.log(`Error occur while removing Classification`, error);
      serverError(error, toastId);
    }
  };

  const defaultColumns = useMemo(
    () => [
      {
        accessorKey: "typeName",
        cell: (info: any) => {
          let values = info.row.original;
          let href: string = `/tag/tagAttribute/${values.typeName}`;

          if (guid != info?.row?.original?.entityGuid) {
            return (
              <Stack direction="row" gap={1}>
                <Link
                  className="entity-name w-100 text-blue text-decoration-none"
                  to={{
                    pathname: href
                  }}
                  color={"primary"}
                >
                  {values.typeName}
                </Link>
                <LightTooltip
                  title={
                    isEntityPurged[values.entityStatus]
                      ? "Entity not available"
                      : ""
                  }
                >
                  <Chip
                    className="chip-items"
                    disabled={
                      isEntityPurged[values.entityStatus] ? true : false
                    }
                    component="a"
                    label={
                      <Link
                        className="entity-name w-100 text-blue text-decoration-none"
                        to={{
                          pathname: `/detailPage/${values?.entityGuid}`,
                          search: `?tabActive=classification`
                        }}
                        color={"primary"}
                      >
                        Propagated From{" "}
                      </Link>
                    }
                    variant="outlined"
                    size="small"
                    color="primary"
                    clickable
                  />
                </LightTooltip>
              </Stack>
            );
          } else {
            return (
              <LightTooltip title={values?.typeName}>
                {" "}
                <Link
                  className="entity-name text-blue text-decoration-none"
                  to={{
                    pathname: href
                  }}
                  color={"primary"}
                >
                  <Typography fontWeight="600"> {values?.typeName} </Typography>
                </Link>
              </LightTooltip>
            );
          }
        },
        header: "Classification",
        width: "30%",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "attributes",
        cell: (info: any) => {
          let values: string[] = info?.row?.original;

          return !isEmpty(values) ? (
            <AttributeTable values={values} />
          ) : (
            <span>N/A</span>
          );
        },
        header: "Attributes",
        enableSorting: false,
        width: "60%"
      },
      {
        accessorKey: "Action",
        cell: (info: any) => {
          let values = info.row.original;
          return (
            <Stack direction="row" gap={1}>
              {(guid == values?.entityGuid ||
                (guid != values?.entityGuid &&
                  values.entityStatus == "DELETED")) && (
                <LightTooltip title={"Delete Classification"}>
                  <CustomButton
                    variant="outlined"
                    color="success"
                    className="table-filter-btn assignTag"
                    size="small"
                    onClick={(e: React.MouseEvent<HTMLElement>) => {
                      e.stopPropagation();
                      setOpenModal(true);
                      let { name } = extractKeyValueFromEntity(entity);
                      setCurrentValue({
                        selectedValue: values.typeName,
                        assetName: name
                      });
                    }}
                    data-cy="addTag"
                  >
                    <DeleteOutlinedIcon className="table-filter-refresh" />
                  </CustomButton>
                </LightTooltip>
              )}
              {guid == values?.entityGuid && (
                <LightTooltip title={"Edit Classification"}>
                  <CustomButton
                    variant="outlined"
                    color="success"
                    className="table-filter-btn assignTag"
                    size="small"
                    onClick={(e: React.MouseEvent<HTMLElement>) => {
                      e.stopPropagation();
                      setRowData(values);
                      setTagModal(true);
                    }}
                    data-cy="addTag"
                  >
                    <EditOutlinedIcon className="table-filter-refresh" />
                  </CustomButton>
                </LightTooltip>
              )}
            </Stack>
          );
        },
        header: "Action",
        width: "10%",
        enableSorting: false
      }
    ],
    [updateTable]
  );

  return (
    <>
      <Grid container marginTop={0} className="properties-container">
        <Grid item md={12} p={2}>
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
                value={classificationName}
                onChange={(_e: any, newValue: string) => {
                  handleChange(newValue as string);
                }}
                isOptionEqualToValue={(option, value) => option === value}
                getOptionLabel={(option) => option}
                options={classificationData}
                id="entity-classification"
                className="classification-table-autocomplete"
                renderInput={(params) => (
                  <TextField {...params} label="Classifications" />
                )}
                disableClearable
              />
              <FormGroup data-cy="checkPropagtedTag">
                <FormControlLabel
                  sx={{ marginRight: "0" }}
                  control={
                    <AntSwitch
                      defaultChecked={
                        !isEmpty(searchParams.get("showPC"))
                          ? getBoolean(searchParams.get("showPC") as string)
                          : true
                      }
                      sx={{ marginRight: "8px" }}
                      size="small"
                      checked={checked}
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                        handleSwitchChange(e);
                      }}
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                      inputProps={{ "aria-label": "controlled" }}
                    />
                  }
                  label="Show Propagated Classifications"
                />
              </FormGroup>
            </Stack>

            <TableLayout
              data={tableData}
              columns={defaultColumns}
              emptyText="No Records found!"
              isFetching={loading}
              columnVisibility={false}
              clientSideSorting={true}
              columnSort={true}
              showPagination={true}
              showRowSelection={false}
              tableFilters={false}
            />
          </Stack>
        </Grid>
      </Grid>
      {openModal && (
        <CustomModal
          open={openModal}
          onClose={handleCloseModal}
          title="Remove Classification Assignment"
          titleIcon={<ErrorRoundedIcon className="remove-modal-icon" />}
          button1Label="Cancel"
          button1Handler={handleCloseModal}
          button2Label="Remove"
          button2Handler={handleRemove}
        >
          <Typography fontSize={15}>
            Remove: <b>{currentValue.selectedValue}</b> assignment from{" "}
            <b>{currentValue.assetName}</b> ?
          </Typography>
        </CustomModal>
      )}
      {tagModal && (
        <AddTag
          open={tagModal}
          isAdd={false}
          entityData={rowdata}
          onClose={handleCloseTagModal}
          setUpdateTable={setUpdateTable}
          setRowSelection={undefined}
        />
      )}
    </>
  );
};

export default ClassificationsTab;
