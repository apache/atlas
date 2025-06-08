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
 * limitations under the License.useForm
 */

import { deleteClassification } from "@api/apiMethods/classificationApiMethod";
import CustomModal from "@components/Modal";
import { useAppDispatch } from "@hooks/reducerHook";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";
import { Typography } from "@mui/material";
import { fetchClassificationData } from "@redux/slice/typeDefSlices/typedefClassificationSlice";
import { serverError } from "@utils/Utils";
import { useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { toast } from "react-toastify";

const DeleteTag = (props: {
  open: boolean;
  onClose: () => void;
  setExpandNode: any;
  node: any;
  updatedData: any;
}) => {
  const { open, onClose, setExpandNode, node, updatedData } = props;
  const navigate = useNavigate();
  const dispatchApi = useAppDispatch();
  const toastId: any = useRef(null);
  const [loader, setLoader] = useState(false);

  const fetchCurrentData = async () => {
    await dispatchApi(fetchClassificationData());
  };
  const handleRemove = async () => {
    try {
      setLoader(true);
      await deleteClassification(node.text);
      updatedData();
      fetchCurrentData();
      toast.success(`Classification ${node.text} was deleted successfully`);
      setLoader(false);
      navigate(
        {
          pathname: "/"
        },
        { replace: true }
      );
      onClose();
      setExpandNode(null);
    } catch (error) {
      setLoader(false);
      console.log(`Error occur while removing ${node.id}`, error);
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
        disableButton2={loader}
      >
        <Typography fontSize={15}>
          Are you sure you want to delete classification
        </Typography>
      </CustomModal>
    </>
  );
};

export default DeleteTag;
