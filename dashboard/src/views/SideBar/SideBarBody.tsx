//@ts-nocheck

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

import * as React from "react";
import atlasLogo from "/img/atlas_logo.svg";
import { Outlet, useNavigate } from "react-router-dom";
import { styled } from "@mui/material/styles";
import Drawer from "@mui/material/Drawer";
import CssBaseline from "@mui/material/CssBaseline";
import MuiAppBar, { AppBarProps as MuiAppBarProps } from "@mui/material/AppBar";
import { EntitiesTree } from "./SideBarTree/EntitiesTree.js";
import { IconButton, LightTooltip } from "@components/muiComponents.js";
import { useSelector } from "react-redux";
import SearchIcon from "@mui/icons-material/Search";
import { CircularProgress, InputBase, Paper, Stack } from "@mui/material";
import { ClassificationTree } from "./SideBarTree/ClassificationTree.js";
import BusinessMetadataTree from "./SideBarTree/BusinessMetadataTree.js";
import GlossaryTree from "./SideBarTree/GlossaryTree.js";
import { TypeHeaderState } from "@models/treeStructureType.js";
import RelationshipsTree from "./SideBarTree/RelationShipsTree.js";
import CustomFiltersTree from "./SideBarTree/CustomFiltersTree.js";
import { Suspense, useState } from "react";
import { globalSessionData } from "@utils/Enum.js";
import KeyboardDoubleArrowLeftIcon from "@mui/icons-material/KeyboardDoubleArrowLeft";
import KeyboardDoubleArrowRightIcon from "@mui/icons-material/KeyboardDoubleArrowRight";
import Header from "@views/Layout/Header.js";

export const defaultDrawerWidth = "25%";

const Main = styled("main", { shouldForwardProp: (prop) => prop !== "open" })<{
  open?: boolean;
}>(({ theme, open }) => ({
  flexGrow: 1,
  padding: theme.spacing(3),
  transition: theme.transitions.create("margin", {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen
  }),
  marginLeft: `-${defaultDrawerWidth}`,
  ...(open && {
    transition: theme.transitions.create("margin", {
      easing: theme.transitions.easing.easeOut,
      duration: theme.transitions.duration.enteringScreen
    }),
    marginLeft: 0
  })
}));

interface AppBarProps extends MuiAppBarProps {
  open?: boolean;
}

const AppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== "open"
})<AppBarProps>(({ open }) => ({
  ...(open && {
    width: `calc(100% - ${defaultDrawerWidth})`,
    marginLeft: `${defaultDrawerWidth}`
  })
}));

const DrawerHeader = styled("div")(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  padding: theme.spacing(0, 1),
  ...theme.mixins.toolbar,
  marginBottom: "1rem"
  // justifyContent: "center"
}));

const SideBarBody = (props: {
  loading: boolean;
  handleOpenModal: any;
  handleOpenAboutModal: any;
}) => {
  const { loading: loader, handleOpenModal, handleOpenAboutModal } = props;
  const navigate = useNavigate();
  const { loading } = useSelector((state: TypeHeaderState) => state.typeHeader);
  const { relationshipSearch = {} } = globalSessionData || {};
  const [open, setOpen] = React.useState(true);
  const [searchTerm, setSearchTerm] = useState<string>("");

  const handleDrawerOpen = () => {
    setOpen(!open);
  };

  const [position, setPosition] = React.useState(defaultDrawerWidth);
  const draggerRef = React.useRef<HTMLDivElement>(null);
  const windowWidth = window.innerWidth;
  const minPosition = 300;
  const maxPosition = windowWidth * 0.6;

  const handleMouseMove = (e: MouseEvent) => {
    let newPosition = e.clientX;

    if (newPosition < minPosition) {
      newPosition = minPosition;
    } else if (newPosition > maxPosition) {
      newPosition = maxPosition;
    }

    setPosition(newPosition);
  };

  const handleMouseUp = () => {
    window.removeEventListener("mousemove", handleMouseMove);
    window.removeEventListener("mouseup", handleMouseUp);
  };

  const handleMouseDown = () => {
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
  };

  React.useEffect(() => {
    const draggerElement = draggerRef.current;

    draggerElement?.addEventListener("mousedown", handleMouseDown);

    return () => {
      draggerElement?.removeEventListener("mousedown", handleMouseDown);
    };
  }, []);

  return (
    <Stack
      flexDirection="row"
      className="sidebar-box"
      sx={{ overflow: "hidden" }}
    >
      <CssBaseline />
      {/* <AppBar
        position="fixed"
        open={open}
        className="sidebar-appbar"
        sx={{
          marginLeft: open == true ? `${position} !important` : 0,
          transform: "translateX(-30px)"
        }}
      >
        <Toolbar className="sidebar-toolbar">
          <IconButton
            className="sidebar-toggle"
            color="inherit"
            aria-label="open drawer"
            onClick={handleDrawerOpen}
            edge="start"
          >
            <MenuIcon />
          </IconButton>
        </Toolbar>
      </AppBar> */}
      <Drawer
        sx={{
          width: position,
          flexShrink: 0,
          // minHeight: "100vh",
          minHeight: "calc(100vh - 64px)",
          minWidth: "30px",
          ...(open == false && {
            transform: `translateX(calc(-${position} + 30px)) !important`
          }),
          ...(open == false && { visibility: "visible !important" }),

          "& .MuiDrawer-paper": {
            background: "#034858",
            boxSizing: "border-box",
            overflow: "hidden",
            position: "fixed",
            // ...(open == false
            //   ? {
            //       top: 0
            //     }
            //   : { top: "62px" }),
            top: "0",
            transition: "none !important",
            ...(open == false && {
              transform: `translateX(30px) !important`
            }),
            ...(open == false && { visibility: "visible !important" })
          }
        }}
        PaperProps={{
          style: { width: position, minWidth: "30px" }
        }}
        variant="persistent"
        anchor="left"
        open={open}
      >
        {/* <Divider /> */}
        <Paper
          className="sidebar-wrapper"
          sx={{
            width: open ? position : "100%",

            ...(open == false && {
              overflow: "hidden"
            })
          }}
        >
          {open && (
            <DrawerHeader>
              <Stack gap="2rem" width="100%">
                <img
                  src={atlasLogo}
                  alt="Atlas logo"
                  onClick={() => {
                    navigate(
                      {
                        pathname: "/search"
                      },
                      { replace: true }
                    );
                  }}
                  className="header-logo"
                  data-cy="atlas-logo"
                />
                <Paper
                  sx={{
                    width: "100%"
                  }}
                  className="sidebar-searchbar"
                >
                  <InputBase
                    fullWidth
                    sx={{ color: "rgba(0, 0, 0, 0.7)" }}
                    placeholder="Entities, Classifications, Glossaries"
                    inputProps={{ "aria-label": "search" }}
                    value={searchTerm}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                      setSearchTerm(e.target.value);
                    }}
                    data-cy="searchNode"
                  />

                  <IconButton type="submit" size="small" aria-label="search">
                    <SearchIcon fontSize="inherit" />
                  </IconButton>
                </Paper>
              </Stack>
            </DrawerHeader>
          )}
          <div
            className="sidebar-treeview-container"
            data-cy="r_entityTreeRender"
          >
            <EntitiesTree
              sideBarOpen={open}
              drawerWidth={position}
              loading={loading}
              searchTerm={searchTerm}
            />
          </div>

          <div
            className="sidebar-treeview-container"
            data-cy="r_classificationTreeRender"
          >
            <ClassificationTree
              sideBarOpen={open}
              drawerWidth={position}
              loading={loader}
              searchTerm={searchTerm}
            />
          </div>

          <div
            className="sidebar-treeview-container"
            data-cy="r_businessMetadataTreeRender"
          >
            <BusinessMetadataTree
              sideBarOpen={open}
              drawerWidth={position}
              searchTerm={searchTerm}
            />
          </div>

          <div
            className="sidebar-treeview-container"
            data-cy="r_glossaryTreeRender"
          >
            <GlossaryTree
              sideBarOpen={open}
              drawerWidth={position}
              searchTerm={searchTerm}
            />
          </div>
          {relationshipSearch && (
            <div
              className="sidebar-treeview-container"
              data-cy="r_relationshipTreeRender"
            >
              <RelationshipsTree
                sideBarOpen={open}
                drawerWidth={position}
                searchTerm={searchTerm}
              />
            </div>
          )}

          <div
            className="sidebar-treeview-container"
            data-cy="r_customFilterTreeRender"
          >
            <CustomFiltersTree
              sideBarOpen={open}
              drawerWidth={position}
              searchTerm={searchTerm}
            />
          </div>
          <div
            style={{
              width: "100%",
              textAlign: "right",
              padding: "4px 0",
              position: "sticky",
              bottom: "0px",
              marginTop: "64px",
              background: "#034858"
              // borderTop: "1px solid rgba(255,255,255,0.15)"
            }}
          >
            <IconButton size="medium" onClick={() => handleDrawerOpen()}>
              {open ? (
                // <LightTooltip title="Collapse">
                <KeyboardDoubleArrowLeftIcon
                  sx={{ color: "white" }}
                  fontSize="medium"
                />
              ) : (
                // </LightTooltip>
                // <LightTooltip title="Expand" direction="right">
                <KeyboardDoubleArrowRightIcon
                  sx={{ color: "white" }}
                  fontSize="medium"
                />
                // </LightTooltip>
              )}
            </IconButton>
          </div>
        </Paper>

        {/* <div
          ref={draggerRef}
          className="sidebar-dragger"
          style={{
            left: position,
            ...(open == false && {
              visibility: "hidden"
            }),
            ...(open == true && {
              visibility: "visible"
            })
          }}
        /> */}
      </Drawer>

      <Main
        open={open}
        sx={{
          ...(open == false && {
            marginLeft: `calc(-${position} + 60px) !important`
          }),
          margin: "0",
          overflowX: "auto",
          background: "#f5f5f5",
          padding: "0"
        }}
      >
        <Stack height="auto" minHeight="100%">
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              backgroundColor: "white",
              height: "56px",
              alignItems: "center",
              padding: "16px"
            }}
          >
            <Header
              handleOpenModal={handleOpenModal}
              handleOpenAboutModal={handleOpenAboutModal}
            />
          </div>
          <div
            style={{
              padding: "16px",
              display: "flex",
              flex: "1",
              flexDirection: "column"
            }}
          >
            <Suspense
              fallback={
                <div
                  style={{
                    position: "fixed",
                    left: 0,
                    top: 0,
                    width: "100%",
                    height: "100vh"
                  }}
                >
                  <CircularProgress
                    disableShrink
                    color="success"
                    sx={{
                      display: "inline-block",
                      position: "absolute",
                      left: "50%",
                      top: "50%",
                      transform: "translate(-50%, -50%)"
                    }}
                  />
                </div>
              }
            >
              <Outlet />
            </Suspense>
          </div>
        </Stack>
      </Main>
    </Stack>
  );
};

export default SideBarBody;
