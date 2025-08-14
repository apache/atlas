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

import { CloseIcon, CustomButton } from "@components/muiComponents";
import SwipeableDrawer from "@mui/material/SwipeableDrawer";
import Stack from "@mui/material/Stack";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import DialogContent from "@mui/material/DialogContent";
import DialogActions from "@mui/material/DialogActions";
import DrawerBodyChipView from "./DrawerBodyChipView";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { toggleDrawer } from "@redux/slice/drawerSlice";

const drawerBleeding = 56;

interface DrawerProps {
  data: Array<{ [key: string]: any }>;
  openDrawer?: boolean;
  toggleDrawer?: (open: boolean) => () => void;
  displayKey: string;
  currentEntity: any;
  removeApiMethod: any;
  removeTagsTitle: string;
  title: string;
  isEditView: boolean;
  isDeleteIcon?: boolean;
}

const ShowMoreDrawer = ({
  data,

  displayKey,
  currentEntity,
  removeApiMethod,
  removeTagsTitle,
  title,
  isEditView,
  isDeleteIcon
}: DrawerProps): JSX.Element => {
  const dispatch = useAppDispatch();
  const { isOpen } = useAppSelector((state) => state.drawerState);

  return (
    <>
      <SwipeableDrawer
        anchor="right"
        open={isOpen}
        onClose={() => {
          dispatch(toggleDrawer());
        }}
        onOpen={() => {
          dispatch(toggleDrawer());
        }}
        swipeAreaWidth={drawerBleeding}
        disableSwipeToOpen={false}
        ModalProps={{
          keepMounted: true
        }}
      >
        <Stack direction="column" width="420px" height="100%">
          <DialogTitle
            sx={{ m: 0, p: 2 }}
            className="modal-title"
            id="customized-dialog-title"
          >
            {title}
          </DialogTitle>
          <IconButton
            aria-label="close"
            sx={(theme) => ({
              position: "absolute",
              right: 8,
              top: 8,
              color: theme.palette.grey[500]
            })}
            onClick={(e: React.MouseEvent) => {
              e.stopPropagation();
              dispatch(toggleDrawer());
            }}
          >
            <CloseIcon />
          </IconButton>
          <DialogContent sx={{ padding: "16px" }} dividers>
            <Stack gap="1rem">
              <DrawerBodyChipView
                data={data}
                currentEntity={currentEntity}
                title={title}
                removeApiMethod={removeApiMethod}
                displayKey={displayKey}
                removeTagsTitle={removeTagsTitle}
                isDeleteIcon={isDeleteIcon}
              />
            </Stack>
          </DialogContent>
          <DialogActions
            sx={{
              position: "sticky",
              bottom: 0
            }}
          >
            {isEditView && (
              <CustomButton
                variant="text"
                color="primary"
                aria-label="save"
                primary={true}
                onClick={(e: Event) => {
                  e.stopPropagation();
                  dispatch(toggleDrawer());
                }}
              >
                Save{" "}
              </CustomButton>
            )}
            <CustomButton
              variant="text"
              color="primary"
              aria-label="close"
              primary={true}
              onClick={(e: Event) => {
                e.stopPropagation();
                dispatch(toggleDrawer());
              }}
            >
              Close{" "}
            </CustomButton>{" "}
          </DialogActions>
        </Stack>
      </SwipeableDrawer>
    </>
  );
};

export default ShowMoreDrawer;
