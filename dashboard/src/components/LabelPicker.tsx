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

import { useTheme, styled } from "@mui/material/styles";
import * as React from "react";
import Popper from "@mui/material/Popper";
import ClickAwayListener from "@mui/material/ClickAwayListener";
import CloseIcon from "@mui/icons-material/Close";
import DoneIcon from "@mui/icons-material/Done";
import Autocomplete, {
  AutocompleteCloseReason,
  autocompleteClasses
} from "@mui/material/Autocomplete";
import InputBase from "@mui/material/InputBase";
import { IconButton, Stack } from "@mui/material";
import { PopperComponentProps } from "../models/globalSearchType";

const StyledAutocompletePopper = styled("div")(({ theme }) => ({
  [`& .${autocompleteClasses.paper}`]: {
    boxShadow: "none",
    margin: 0,
    color: "inherit",
    fontSize: 13
  },
  [`& .${autocompleteClasses.listbox}`]: {
    backgroundColor: theme.palette.mode === "light" ? "#fff" : "#1c2128",
    padding: 0,
    [`& .${autocompleteClasses.option}`]: {
      minHeight: "auto",
      alignItems: "flex-start",
      padding: 8,
      borderBottom: `1px solid  ${
        theme.palette.mode === "light" ? " #eaecef" : "#30363d"
      }`,
      '&[aria-selected="true"]': {
        backgroundColor: "transparent"
      },
      [`&.${autocompleteClasses.focused}, &.${autocompleteClasses.focused}[aria-selected="true"]`]:
        {
          backgroundColor: theme.palette.action.hover
        }
    }
  },
  [`&.${autocompleteClasses.popperDisablePortal}`]: {
    position: "relative"
  }
}));

function PopperComponent(props: PopperComponentProps) {
  const { disablePortal, anchorEl, open, ...other } = props;
  return <StyledAutocompletePopper {...other} />;
}

const StyledPopper = styled(Popper)(({ theme }) => ({
  border: `1px solid ${theme.palette.mode === "light" ? "#e1e4e8" : "#30363d"}`,
  boxShadow: `0 8px 24px ${
    theme.palette.mode === "light" ? "rgba(149, 157, 165, 0.2)" : "rgb(1, 4, 9)"
  }`,
  borderRadius: 6,
  width: 300,
  zIndex: theme.zIndex.modal,
  fontSize: 13,
  color: theme.palette.mode === "light" ? "#24292e" : "#c9d1d9",
  backgroundColor: theme.palette.mode === "light" ? "#fff" : "#1c2128"
}));

const StyledInput = styled(InputBase)(({ theme }) => ({
  padding: 10,
  width: "100%",
  borderBottom: `1px solid ${
    theme.palette.mode === "light" ? "#eaecef" : "#30363d"
  }`,
  "& input": {
    borderRadius: 4,
    backgroundColor: theme.palette.mode === "light" ? "#fff" : "#0d1117",
    padding: 8,
    transition: theme.transitions.create(["border-color", "box-shadow"]),
    border: `1px solid ${
      theme.palette.mode === "light" ? "#eaecef" : "#30363d"
    }`,
    fontSize: 14,
    "&:focus": {
      boxShadow: `0px 0px 0px 3px ${
        theme.palette.mode === "light"
          ? "rgba(3, 102, 214, 0.3)"
          : "rgb(12, 45, 107)"
      }`,
      borderColor: theme.palette.mode === "light" ? "#0366d6" : "#388bfd"
    }
  }
}));

const LabelPicker = (props: {
  anchorEl: any;
  Label: any;
  handleCloseLabelPicker: any;
  value: any;
  id: any;
  pendingValue: any;
  setPendingValue: any;
  handleClickLabelPicker: any;
  optionList: any;
}) => {
  const {
    anchorEl,
    Label,
    handleCloseLabelPicker,
    value,
    id,
    pendingValue,
    setPendingValue,
    handleClickLabelPicker,
    optionList
  } = props;

  const theme = useTheme();

  const open = Boolean(anchorEl);

  return (
    <React.Fragment>
      <IconButton
        aria-label="Filters"
        disableRipple
        size="small"
        className="label-picker-icon-btn"
        aria-describedby={id}
        onClick={handleClickLabelPicker}
      >
        {Label}
      </IconButton>
      <StyledPopper
        id={id}
        open={open}
        anchorEl={anchorEl}
        placement="bottom-start"
      >
        <ClickAwayListener
          mouseEvent="onMouseDown"
          touchEvent="onTouchStart"
          onClickAway={handleCloseLabelPicker}
        >
          <div>
            <Stack
              sx={{
                borderBottom: `1px solid ${
                  theme.palette.mode === "light" ? "#eaecef" : "#30363d"
                }`
              }}
              className="label-picker-name"
            >
              Select Filters
            </Stack>
            <Autocomplete
              open
              size="small"
              multiple
              onClose={(
                _event: React.ChangeEvent<{}>,
                reason: AutocompleteCloseReason
              ) => {
                if (reason === "escape") {
                  handleCloseLabelPicker();
                }
              }}
              value={pendingValue}
              isOptionEqualToValue={(option, value) => option === value}
              onChange={(event, newValue, reason) => {
                if (
                  event.type === "keydown" &&
                  ((event as React.KeyboardEvent).key === "Backspace" ||
                    (event as React.KeyboardEvent).key === "Delete") &&
                  reason === "removeOption"
                ) {
                  return;
                }
                setPendingValue(newValue);
              }}
              disableCloseOnSelect
              PopperComponent={PopperComponent}
              renderTags={() => null}
              noOptionsText="No results found"
              renderOption={(props, option, { selected }) => {
                const { key, ...optionProps }: any = props;
                return (
                  <li key={key} {...optionProps}>
                    <Stack
                      component={DoneIcon}
                      className="label-picker-list"
                      style={{
                        visibility: selected ? "visible" : "hidden"
                      }}
                    />

                    <Stack
                      sx={(t) => ({
                        flexGrow: 1,
                        "& span": {
                          color: "#8b949e",
                          ...t.applyStyles("light", {
                            color: "#586069"
                          })
                        }
                      })}
                    >
                      {option}
                    </Stack>
                    <Stack
                      component={CloseIcon}
                      className="label-picker-close-icon"
                      style={{
                        visibility: selected ? "visible" : "hidden"
                      }}
                    />
                  </li>
                );
              }}
              options={optionList.sort((a: string, b: string) => {
                let ai = value.indexOf(a);
                ai = ai === -1 ? value.length + optionList.indexOf(a) : ai;
                let bi = value.indexOf(b);
                bi = bi === -1 ? value.length + optionList.indexOf(b) : bi;
                return ai - bi;
              })}
              getOptionLabel={(option) => option}
              renderInput={(params) => (
                <StyledInput
                  ref={params.InputProps.ref}
                  inputProps={params.inputProps}
                  autoFocus
                  placeholder="Filter labels"
                />
              )}
            />
          </div>
        </ClickAwayListener>
      </StyledPopper>
    </React.Fragment>
  );
};

export default LabelPicker;
