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

import { ChangeEvent, useRef, useState } from "react";
import {
  Divider,
  IconButton,
  LightTooltip,
  Menu,
  MenuItem,
  Typography
} from "@components/muiComponents";
import DownloadForOfflineRoundedIcon from "@mui/icons-material/DownloadForOfflineRounded";
import BarChartOutlinedIcon from "@mui/icons-material/BarChartOutlined";
import AccountCircleRoundedIcon from "@mui/icons-material/AccountCircleRounded";
import { useLocation, useNavigate } from "react-router-dom";
import QuickSearch from "@components/GlobalSearch/QuickSearch";
import { Logout } from "@mui/icons-material";
import { useAppSelector } from "@hooks/reducerHook";
import {
  Button,
  List,
  ListItem,
  ListItemText,
  Popover,
  Skeleton,
  Stack
} from "@mui/material";
import DownloadIcon from "@mui/icons-material/Download";
import RefreshIcon from "@mui/icons-material/Refresh";
import CloseOutlinedIcon from "@mui/icons-material/CloseOutlined";
import { getDownloadStatus } from "@api/apiMethods/downloadApiMethod";
import {
  getBaseUrl,
  getNavigate,
  isEmpty,
  serverError,
  setNavigate
} from "@utils/Utils";
import { toast } from "react-toastify";
import { apiDocUrl } from "@api/apiUrlLinks/headerUrl";
import { globalSessionData } from "@utils/Enum";
import { downloadSearchResultsFileUrl } from "@api/apiUrlLinks/downloadApiUrl";
import ChevronLeftIcon from "@mui/icons-material/ChevronLeft";
import { AntSwitch } from "@utils/Muiutils";

interface Header {
  handleOpenModal: () => void;
  handleOpenAboutModal: () => void;
}

const Header: React.FC<Header> = ({
  handleOpenModal,
  handleOpenAboutModal
}) => {
  const location = useLocation();
  const navigate = useNavigate();
  const toastId: any = useRef(null);
  const { sessionObj = "" }: any = useAppSelector(
    (state: any) => state.session
  );
  const { userName } = sessionObj.data || {};
  const { debugMetrics = {} } = globalSessionData || {};

  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [nestedAnchorEl, _setNestedAnchorEl] = useState(null);
  const [_helpMenu, setHelpmenu] = useState<null | HTMLElement>(null);
  const [openNestedMenu, setOpenNestedMenu] = useState(false);

  const open = Boolean(anchorEl);

  const [downloadAnchorEl, setDownloadAnchorEl] = useState<null | HTMLElement>(
    null
  );
  const [searchDownloadsList, setSearchDownList] = useState([]);
  const [downloadLoader, setDownloadLoader] = useState<boolean>(false);
  const [checked, setChecked] = useState(false);

  if (location.pathname == "/search/searchResult") {
    setNavigate(location.pathname + location.search);
  }

  const fetchDownloadStatus = async () => {
    try {
      setDownloadLoader(true);
      const downListResp = await getDownloadStatus({});
      const { searchDownloadRecords } = downListResp.data;
      setSearchDownList(searchDownloadRecords);
      setDownloadLoader(false);
    } catch (error) {
      setDownloadLoader(false);
      console.error(`Error occur while fetching searchResult records`, error);
      serverError(error, toastId);
    }
  };

  const handleClickDownload = (event: React.MouseEvent<HTMLElement>) => {
    setDownloadAnchorEl(event.currentTarget);
    fetchDownloadStatus();
  };

  const handleCloseDownload = () => {
    setDownloadAnchorEl(null);
  };

  const openPopover = Boolean(downloadAnchorEl);

  const handleClose = () => {
    setAnchorEl(null);
    handleNestedMenuClose();
  };
  const handleClick = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleFileDownload = async (file: any) => {
    const { fileName } = file;
    let path = getBaseUrl(window.location.pathname);
    window.location.href = path + downloadSearchResultsFileUrl(fileName);
    toast.success("File download succesfully");
  };

  const handleNestedMenuClick = () => {
    setOpenNestedMenu(!openNestedMenu);
  };

  const handleNestedMenuClose = () => {
    setOpenNestedMenu(false);
    setHelpmenu(null);
  };

  const handleLogout = () => {
    localStorage.setItem("atlas_ui", "beta");
    let path = getBaseUrl(window.location.pathname);
    window.location.href = path + "/logout.html";
    handleClose();
  };

  const handleSwitchChange = (e: ChangeEvent<HTMLInputElement>) => {
    setChecked(e.target.checked);
  };

  return (
    <>
      {(location.pathname.includes("detailPage") ||
        location.pathname.includes("tag") ||
        location.pathname.includes("glossary") ||
        location.pathname.includes("relationshipDetailPage")) && (
        <LightTooltip title="Back to search page">
          <Button
            onClick={() => {
              navigate(getNavigate());
            }}
            data-cy="backToSearch"
            size="small"
            variant="text"
            className="text-black-default"
          >
            <ChevronLeftIcon /> <Typography>Back</Typography>
          </Button>
        </LightTooltip>
      )}
      {location.pathname != "/" &&
        location.pathname != "/search" &&
        location.pathname != "/!" &&
        (!location.pathname.includes("!") ) && (
          <div style={{ display: "flex", justifyContent: "center", flex: "1" }}>
            <QuickSearch />
          </div>
        )}
      <div className="header-menu">
        <LightTooltip title="Downloads">
          <IconButton
            size="small"
            className="header-icon-button text-black-default"
            data-cy="showDownloads"
            onClick={handleClickDownload}
          >
            <DownloadForOfflineRoundedIcon />
          </IconButton>
        </LightTooltip>
        <LightTooltip title="Statistics">
          <IconButton
            size="small"
            className="header-icon-button text-black-default"
            data-cy="showStats"
            onClick={handleOpenModal}
          >
            <BarChartOutlinedIcon fontSize="small" />
          </IconButton>
        </LightTooltip>

        <Button
          onClick={handleClick}
          size="small"
          aria-controls={open ? "account-menu" : undefined}
          aria-haspopup="true"
          aria-expanded={open ? "true" : undefined}
          className="header-icon-button profile-button text-black-default"
          data-cy="user-account"
        >
          <AccountCircleRoundedIcon fontSize="small" />
          <span className="header-username">{userName}</span>
        </Button>

        <Menu
          anchorEl={anchorEl}
          id="account-menu"
          open={open}
          onClose={handleClose}
          slotProps={{
            paper: {
              elevation: 0,
              sx: {
                overflow: "visible",
                filter: "drop-shadow(0px 2px 8px rgba(0,0,0,0.32))",
                mt: 1.5,
                "&::before": {
                  content: '""',
                  display: "block",
                  position: "absolute",
                  top: 0,
                  right: 14,
                  width: 10,
                  height: 10,
                  bgcolor: "background.paper",
                  transform: "translateY(-50%) rotate(45deg)",
                  zIndex: 0
                }
              }
            }
          }}
          transformOrigin={{ horizontal: "right", vertical: "top" }}
          anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
        >
          <MenuItem
            dense
            onClick={() => {
              navigate(
                {
                  pathname: "/administrator"
                },
                { replace: true }
              );
              handleClose();
            }}
            data-cy="administrator"
          >
            Administration
          </MenuItem>
          <MenuItem dense onClick={handleNestedMenuClick} data-cy="help">
            Help
          </MenuItem>
          <Divider />
          <MenuItem
            dense
            data-cy="classicUI"
            onClick={() => {
              let path = getBaseUrl(window.location.pathname);
              window.location.href = path + "/index.html#!";
              handleClose();
            }}
          >
            Switch to Classic
          </MenuItem>
           <MenuItem
            dense
            data-cy="newUI"
            onClick={() => {
              let path = getBaseUrl(window.location.pathname);
              window.location.href = path + "/n/index.html#!";
              handleClose();
            }}
          >
            Switch to New
          </MenuItem>
          <MenuItem
            dense
            data-cy="signOut"
            onClick={() => {
              handleLogout();
            }}
          >
            <Logout sx={{ marginRight: "4px" }} fontSize="inherit" />
            Logout
          </MenuItem>
        </Menu>

        <Popover
          open={openNestedMenu}
          anchorEl={nestedAnchorEl}
          onClose={handleNestedMenuClose}
          anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
          transformOrigin={{ vertical: "top", horizontal: "right" }}
          sx={{ transform: "translate(-134px, 40px)" }}
        >
          <List dense>
            <ListItem
              button
              component="a"
              href="http://atlas.apache.org/"
              target="_blank"
            >
              <ListItemText primary="Documentation" />
            </ListItem>
            <ListItem button component="a" href={apiDocUrl()} target="_blank">
              <ListItemText primary="API Documentation" />
            </ListItem>
            <ListItem
              button
              onClick={() => {
                handleOpenAboutModal();
                handleClose();
              }}
            >
              <ListItemText primary="About" />
            </ListItem>
            {debugMetrics && (
              <ListItem
                button
                data-cy="showDebug"
                onClick={() => {
                  navigate(
                    {
                      pathname: "/debugMetrics"
                    },
                    { replace: true }
                  );
                  handleClose();
                }}
              >
                <ListItemText primary="Debug" />
              </ListItem>
            )}
          </List>
        </Popover>
      </div>
      <Popover
        open={openPopover}
        anchorEl={downloadAnchorEl}
        onClose={handleCloseDownload}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "center"
        }}
        transformOrigin={{
          vertical: "top",
          horizontal: "center"
        }}
      >
        <Stack
          spacing={1}
          sx={{
            maxWidth: "600px",
            minWidth: "400px",
            padding: "16px",
            borderRadius: "4px"
          }}
        >
          <Stack
            direction="row"
            justifyContent="space-between"
            alignItems="center"
            sx={{ paddingBottom: "8px", borderBottom: "1px solid #ccc" }}
          >
            <Typography fontWeight={600}>Downloads</Typography>
            <Stack direction="row" alignItems="center" gap="0.25rem">
              <LightTooltip
                title={
                  checked ? "Display All Files" : "Display Available Files"
                }
              >
                <AntSwitch
                  sx={{ marginRight: "4px" }}
                  size="small"
                  checked={checked}
                  onChange={(e) => {
                    handleSwitchChange(e);
                  }}
                  onClick={(e) => {
                    e.stopPropagation();
                  }}
                  inputProps={{ "aria-label": "controlled" }}
                />
              </LightTooltip>
              <IconButton
                size="small"
                color="primary"
                onClick={() => fetchDownloadStatus()}
              >
                <RefreshIcon fontSize="small" />
              </IconButton>
              <IconButton
                size="small"
                color="primary"
                onClick={() => {
                  handleCloseDownload();
                }}
              >
                <CloseOutlinedIcon fontSize="small" />
              </IconButton>
            </Stack>
          </Stack>

          <Stack sx={{ maxHeight: "600px", overflowY: "auto" }}>
            {downloadLoader ? (
              <List dense disablePadding>
                {[...Array(2)].map((_, index) => (
                  <ListItem dense key={index} disablePadding>
                    <Stack
                      direction="row"
                      spacing={1}
                      alignItems="center"
                      sx={{ width: "100%" }}
                    >
                      <Skeleton variant="text" width="100%" />
                    </Stack>
                  </ListItem>
                ))}
              </List>
            ) : !isEmpty(searchDownloadsList) ? (
              searchDownloadsList?.map((file: { fileName: string }, index) => {
                return (
                  <List dense disablePadding>
                    <ListItem dense disablePadding>
                      <Stack
                        direction="row"
                        spacing={2}
                        alignItems="center"
                        sx={{ width: "100%" }}
                      >
                        <i
                          className="fa fa-file-excel-o fa-lg text-color-green"
                          aria-hidden="true"
                        ></i>
                        <ListItemText primary={file.fileName} />
                        <IconButton
                          color="success"
                          onClick={() => handleFileDownload(file)}
                        >
                          <DownloadIcon />
                        </IconButton>
                      </Stack>
                    </ListItem>
                    {index != searchDownloadsList?.length - 1 && <Divider />}
                  </List>
                );
              })
            ) : (
              <Typography>No Data Found</Typography>
            )}
          </Stack>
        </Stack>
      </Popover>
    </>
  );
};

export default Header;
