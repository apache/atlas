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

import { CustomButton } from "@components/muiComponents";
import { Grid, Stack, Typography } from "@mui/material";
import { useMemo, useRef, useState } from "react";
import VisibilityIcon from "@mui/icons-material/Visibility";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import { termRelationAttributeList } from "@utils/Enum";
import DialogShowMoreLess from "@components/DialogShowMoreLess";
import {
  assignGlossaryType,
  removeTerm
} from "@api/apiMethods/glossaryApiMethod";
import CustomModal from "@components/Modal";
import { isEmpty, serverError } from "@utils/Utils";
import { TableLayout } from "@components/Table/TableLayout";
import moment from "moment";
import TermRelationAttributes from "./TermRelationAttributes";
import { useForm } from "react-hook-form";
import { cloneDeep } from "@utils/Helper";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";
import { useLocation, useParams } from "react-router-dom";
import { toast } from "react-toastify";
import { useAppDispatch } from "@hooks/reducerHook";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";

const TermRelation = ({ glossaryTypeData }: any) => {
  const location = useLocation();
  const toastId: any = useRef(null);
  const dispatchApi = useAppDispatch();
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");
  const { guid: entityGuid } = useParams();
  const [openViewModal, setOpenViewModal] = useState<boolean>(false);
  const [editModal, setEditModal] = useState<boolean>(false);

  const [currentType, setCurrentType] = useState("");
  const [termObj, setTermObj] = useState([]);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const {
    control,
    handleSubmit,
    formState: { isSubmitting }
  } = useForm();

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    let glossariesData = cloneDeep(glossaryTypeData);
    let relationTypes = Object.keys(formData)[0];

    let relationTypesData = glossariesData[relationTypes].map(
      (obj: { displayText: string | number }) => {
        return { ...obj, ...formData[relationTypes][obj.displayText] };
      }
    );

    glossariesData[relationTypes] = relationTypesData;

    try {
      await assignGlossaryType(entityGuid as string, glossariesData);

      if (!isEmpty(entityGuid)) {
        let params: any = { gtype: gType, guid: entityGuid };
        dispatchApi(fetchGlossaryDetails(params));
        dispatchApi(fetchDetailPageData(entityGuid as string));
      }
      toast.dismiss(toastId.current);
      toastId.current = toast.success(`Attributes updated successfully`);
      handleCloseModal;
    } catch (error) {
      console.log(`Error occur while updating Attributes`, error);
      serverError(error, toastId);
    }
  };

  const handleCloseModal = () => {
    setOpenViewModal(false);
  };

  const handleClick = (row: string) => {
    let rowData = glossaryTypeData[row];
    setCurrentType(row);
    setTermObj(rowData);
  };

  const defaultColumns = useMemo(
    () => [
      {
        accessorKey: "types",
        cell: (info: any) => {
          let values = info.row.original;
          return <Typography fontWeight="600"> {values} </Typography>;
        },
        header: "Relation Types",
        width: "30%",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "terms",
        cell: (info: any) => {
          let values: string = info.row.original;

          return (
            <Stack direction="row" gap={1}>
              <DialogShowMoreLess
                value={glossaryTypeData}
                readOnly={false}
                setUpdateTable={setUpdateTable}
                columnVal={values}
                colName="Term"
                relatedTerm={true}
                displayText="qualifiedName"
                removeApiMethod={removeTerm}
                isShowMoreLess={false}
                detailPage={true}
              />
            </Stack>
          );
        },
        header: "Related Terms",
        enableSorting: false,
        width: "60%"
      },
      {
        accessorKey: "attributes",
        cell: (info: any) => {
          let values: string = info.row.original;
          return (
            !isEmpty(glossaryTypeData) &&
            !isEmpty(glossaryTypeData?.[values]) && (
              <Stack direction="row" gap={1}>
                <CustomButton
                  variant="outlined"
                  color="success"
                  className="table-filter-btn assignTag"
                  size="small"
                  onClick={(_e: React.MouseEvent<HTMLElement>) => {
                    setEditModal(false);
                    setOpenViewModal(true);
                    handleClick(values);
                  }}
                  data-cy="showAttribute"
                >
                  <VisibilityIcon className="table-filter-refresh" />
                </CustomButton>

                <CustomButton
                  variant="outlined"
                  color="success"
                  className="table-filter-btn assignTag"
                  size="small"
                  onClick={(_e: React.MouseEvent<HTMLElement>) => {
                    setEditModal(true);
                    handleClick(values);
                    setOpenViewModal(true);
                  }}
                  data-cy="editAttribute"
                >
                  <EditOutlinedIcon className="table-filter-refresh" />
                </CustomButton>
              </Stack>
            )
          );
        },
        header: "Attributes",
        width: "10%",
        enableSorting: false
      }
    ],
    [updateTable]
  );

  return (
    <>
      <Grid
        container
        marginTop={0}
        className="properties-container"
        borderRadius="4px"
      >
        <Grid item md={12} p={2}>
          <Stack>
            <TableLayout
              data={Object.keys(termRelationAttributeList)}
              columns={defaultColumns}
              emptyText="No Records found!"
              columnVisibility={false}
              clientSideSorting={false}
              columnSort={false}
              showPagination={false}
              showRowSelection={false}
              tableFilters={false}
            />
          </Stack>
        </Grid>
      </Grid>
      {openViewModal && (
        <CustomModal
          open={openViewModal}
          onClose={handleCloseModal}
          title={`${editModal ? "Edit" : ""} Attributes of ${currentType}`}
          button1Label={editModal ? "Close" : undefined}
          button1Handler={editModal ? handleCloseModal : () => undefined}
          button2Label={editModal ? "Update" : "Close"}
          button2Handler={editModal ? handleSubmit(onSubmit) : handleCloseModal}
          disableButton2={isSubmitting}
          maxWidth="md"
        >
          <form onSubmit={handleSubmit(onSubmit)}>
            <TermRelationAttributes
              editModal={editModal}
              termObj={termObj}
              control={control}
              currentType={currentType}
            />
          </form>
        </CustomModal>
      )}
    </>
  );
};

export default TermRelation;
