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
  deleteGlossaryorTerm,
  deleteGlossaryorType
} from "@api/apiMethods/glossaryApiMethod";
import CustomModal from "@components/Modal";
import { useAppDispatch } from "@hooks/reducerHook";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";
import { Typography } from "@mui/material";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";
import { isEmpty, serverError } from "@utils/Utils";
import { useRef } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import { toast } from "react-toastify";

const DeleteGlossary = (props: {
  open: boolean;
  onClose: () => void;
  setExpandNode: any;
  node: any;
  updatedData: any;
}) => {
  const { open, onClose, setExpandNode, node, updatedData } = props;
  const { id, guid, cGuid, types } = node;
  const gtype: string | undefined | null =
    types != "parent" ? "term" : "glossary";
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const glossaryType = searchParams.get("gtype");
  const { guid: glossaryGuid } = useParams();

  const navigate = useNavigate();
  const dispatchApi = useAppDispatch();
  const toastId: any = useRef(null);

  const fetchCurrentData = async () => {
    await dispatchApi(fetchGlossaryData());
  };

  const handleRemove = async () => {
    try {
      gtype == "term"
        ? await deleteGlossaryorType(cGuid)
        : await deleteGlossaryorTerm(guid);
      updatedData();
      fetchCurrentData();
      toast.success(
        `${
          gtype == "term" ? "Term" : "Glossary"
        } ${id} was deleted successfully`
      );
      if (!isEmpty(glossaryGuid) || !isEmpty(glossaryType)) {
        navigate(
          {
            pathname: "/"
          },
          { replace: true }
        );
      }
      onClose();
      setExpandNode(null);
    } catch (error) {
      console.log(`Error occur while removing ${id}`, error);
      serverError(error, toastId);
    }
  };

  return (
    <>
      <CustomModal
        open={open}
        onClose={onClose}
        title="Confirmation"
        titleIcon={<ErrorRoundedIcon className="remove-modal-icon" />}
        button1Label="Cancel"
        button1Handler={onClose}
        button2Label="Ok"
        maxWidth="sm"
        button2Handler={handleRemove}
      >
        <Typography fontSize={15}>
          Are you sure you want to delete{" "}
          {gtype == "term" ? "Term" : "Glossary"}
        </Typography>
      </CustomModal>
    </>
  );
};

export default DeleteGlossary;
