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
import CircularProgress from "@mui/material/CircularProgress";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import { CloseIcon, CustomButton } from "./muiComponents";
import { isEmpty } from "../utils/Utils";

const BootstrapDialog = styled(Dialog)(() => ({
  "& .MuiDialogContent-root": {
    position: "relative",
    maxHeight: "calc(100vh - 215px)",
    minHeight: 70,
    overflow: "auto",
    width: "100% !important"
  },
  "& .MuiDialogActions-root": {},
  "& .MuiDialog-paper": {
    width: "100% !important",
    position: "absolute",
    top: 10
  }
}));
interface CustomModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  titleIcon?: any;
  postTitleIcon?: any;
  children?: any;
  button1Label?: string;
  button1Handler: any;
  button2Label?: string | undefined;
  button2Handler: any;
  disableButton2?: string | object | boolean;
  maxWidth?: any;
  footer?: boolean;
  isDirty?: boolean;
}

export const CustomModal: React.FC<CustomModalProps> = ({
  open,
  onClose,
  title,
  titleIcon,
  postTitleIcon,
  children,
  button1Label,
  button1Handler,
  button2Label,
  button2Handler,
  disableButton2,
  maxWidth,
  isDirty,
  footer
}) => {
  const handleClick = (event: { stopPropagation: () => void }) => {
    event.stopPropagation();
  };
  return (
    <>
      <div className="modal-content" onClick={handleClick}>
        <BootstrapDialog
          maxWidth={maxWidth || "sm"}
          aria-labelledby="customized-dialog-title"
          open={open}
          sx={{
            "& .MuiDialog-paper": {
              // maxWidth: "80% !important",
              // width: "50% !important",
              position: "absolute",
              top: 10
            }
          }}
        >
          <Stack
            direction="row"
            justifyContent="space-between"
            alignItems="center"
            padding="16px"
          >
            <DialogTitle
              id="customized-dialog-title"
              sx={{
                padding: "0",
                display: "flex",
                alignItems: "center"
              }}
            >
              <Stack direction="row" alignItems="center" gap="0.5rem">
                {titleIcon != undefined && titleIcon}
                <Typography className={"modal-title"} sx={{ color: "#333" }}>
                  {title}
                </Typography>
                {postTitleIcon != undefined && postTitleIcon}
              </Stack>
            </DialogTitle>
            <Stack>
              <IconButton
                aria-label="close"
                sx={{
                  color: (theme) => theme.palette.grey[500]
                }}
                onClick={(e) => {
                  e.stopPropagation();
                  onClose();
                }}
              >
                <CloseIcon sx={{ width: "0.75em", height: "0.75em" }} />
              </IconButton>
            </Stack>
          </Stack>
          <DialogContent dividers>{children}</DialogContent>
          {footer == undefined && (
            <Stack padding="16px">
              <DialogActions
                sx={{
                  padding: "0",
                  display: "flex",
                  alignItems: "center"
                }}
              >
                {!isEmpty(button1Label) && (
                  <CustomButton
                    variant="outlined"
                    color="primary"
                    onClick={(e: Event) => {
                      e.stopPropagation();
                      button1Handler();
                    }}
                  >
                    {button1Label}
                  </CustomButton>
                )}

                <CustomButton
                  variant="contained"
                  color="primary"
                  aria-label="close"
                  primary={true}
                  disabled={
                    disableButton2 || (isDirty != undefined ? !isDirty : false)
                  }
                  sx={{
                    ...(disableButton2 && {
                      "&.Mui-disabled": {
                        pointerEvents: "unset",
                        cursor: "not-allowed"
                      }
                    })
                  }}
                  startIcon={
                    disableButton2 && (
                      <CircularProgress
                        sx={{ color: "white", fontWeight: "600" }}
                        size="14px"
                      />
                    )
                  }
                  onClick={(e: Event) => {
                    e.stopPropagation();
                    button2Handler();
                  }}
                >
                  {button2Label}
                </CustomButton>
              </DialogActions>
            </Stack>
          )}
        </BootstrapDialog>
      </div>
    </>
  );
};

export default CustomModal;
