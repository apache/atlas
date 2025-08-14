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

import { styled } from "@mui/material/styles";
import * as React from "react";
import { useRef, useState } from "react";
import {
  CloseIcon,
  CustomButton,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  LightTooltip,
  Typography
} from "./muiComponents";
import Stack from "@mui/material/Stack";
import Dropzone from "../views/SideBar/Import/ImportLayout";
import { getBusinessMetadataImport } from "../api/apiMethods/entitiesApiMethods";
import { toast } from "react-toastify";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemText from "@mui/material/ListItemText";
import ArrowBackIosNewIcon from "@mui/icons-material/ArrowBackIosNew";
import { getGlossaryImport } from "../api/apiMethods/glossaryApiMethod";

const BootstrapDialog = styled(Dialog)(({ theme }) => ({
  "& .MuiDialogContent-root": {
    padding: theme.spacing(2),
    position: "relative",
    maxHeight: "calc(100vh - 215px)",
    minHeight: 70,
    overflow: "auto",
    width: 600
  },
  "& .MuiDialogActions-root": {
    padding: theme.spacing(1)
  },
  "& .MuiDialog-paper": {
    width: 600,
    position: "absolute",
    top: 20
  }
}));
interface CustomModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
}

export const ImportDialog: React.FC<CustomModalProps> = ({
  open,
  onClose,
  title
}) => {
  const [fileData, setFileData] = useState<any>([]);
  const [progress, setProgress] = useState(0);
  const [errorDetails, setErrorDetails] = useState(false);
  const [importData, setImportData] = useState<any>(null);
  const toastId: any = useRef(null);

  const onUpload = async () => {
    if (fileData) {
      try {
        let apiMethod =
          title == "Import Business Template"
            ? getBusinessMetadataImport
            : getGlossaryImport;
        let formData = new FormData();
        formData.append("file", fileData);
        const importResp = await apiMethod(formData, {
          onUploadProgress: (progressEvent: {
            loaded: number;
            total: number;
          }) => {
            let progressValue =
              (progressEvent.loaded / progressEvent.total) * 100;
            setProgress(progressValue);
          }
        });

        if (importResp.data.failedImportInfoList == undefined) {
          toast.dismiss(toastId.current);
          toastId.current = toast.success(
            `File: ${fileData.name} imported successfully`
          );

          onClose();
          setErrorDetails(false);
          setFileData([]);
        }
        if (importResp.data.failedImportInfoList != undefined) {
          toast.dismiss(toastId.current);
          toastId.current = toast.error(
            importResp.data.failedImportInfoList[0].remarks
          );

          setErrorDetails(true);
        }
        setImportData(importResp.data);
      } catch (error) {
        toast.dismiss(toastId.current);
        toastId.current = toast.error(`Invalid JSON response from server`);
      }
    }
  };

  return (
    <React.Fragment>
      <BootstrapDialog aria-labelledby="customized-dialog-title" open={open}>
        <DialogTitle
          variant="h6"
          className={`${!errorDetails ? "modal-title" : "modal-error-title"}`}
          id="customized-dialog-title"
        >
          {!errorDetails ? (
            title
          ) : (
            <>
              <IconButton
                aria-label="back"
                onClick={() => {
                  setErrorDetails(false);
                }}
                sx={{
                  display: "inline-flex",
                  position: "relative",
                  color: (theme) => theme.palette.grey[500]
                }}
              >
                <LightTooltip title="Back to import file">
                  <ArrowBackIosNewIcon sx={{ fontSize: "1.25rem" }} />
                </LightTooltip>
              </IconButton>
              <Typography>Error Details</Typography>
            </>
          )}
        </DialogTitle>
        <IconButton
          aria-label="close"
          onClick={() => {
            onClose();
            setErrorDetails(false);
            setFileData([]);
          }}
          className="modal-close-icon"
          sx={{
            color: (theme) => theme.palette.grey[500]
          }}
        >
          <CloseIcon />
        </IconButton>
        <DialogContent dividers>
          <Typography gutterBottom>
            {!errorDetails ? (
              <Dropzone
                setFileData={setFileData}
                progressVal={progress}
                setProgress={setProgress}
                selectedFile={
                  fileData !== undefined && Object.keys(fileData).length > 0
                    ? [fileData]
                    : []
                }
                errorDetails={errorDetails}
              />
            ) : (
              <Stack
                sx={{
                  width: "100%",
                  height: 400,
                  maxWidth: 360,
                  bgcolor: "background.paper"
                }}
              >
                <List>
                  {importData.failedImportInfoList.map(
                    (
                      value: {
                        index: number;
                        remarks: string;
                      },
                      index: number
                    ) => (
                      <ListItem key={value.index} disableGutters disablePadding>
                        <ListItemText
                          className="dropzone-listitem"
                          primary={`${index + 1}. ${value.remarks}`}
                        />
                      </ListItem>
                    )
                  )}
                </List>
              </Stack>
            )}
          </Typography>
        </DialogContent>
        <DialogActions>
          <CustomButton
            variant="outlined"
            color="primary"
            onClick={() => {
              onClose();
              setErrorDetails(false);
              setFileData([]);
            }}
          >
            Cancel
          </CustomButton>

          {!errorDetails && (
            <CustomButton
              variant="contained"
              color="primary"
              sx={{
                cursor:
                  fileData !== undefined && Object.keys(fileData).length > 0
                    ? "default"
                    : "not-allowed"
              }}
              onClick={() => onUpload()}
            >
              Upload
            </CustomButton>
          )}
        </DialogActions>
      </BootstrapDialog>
    </React.Fragment>
  );
};

export default ImportDialog;
