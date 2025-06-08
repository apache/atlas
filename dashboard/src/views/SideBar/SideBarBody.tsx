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
import {
  Suspense,
  useEffect,
  useState,
  ChangeEvent,
  lazy,
  useRef
} from "react";
import atlasLogo from "/img/atlas_logo.svg";
import {
  matchRoutes,
  Outlet,
  RouteObject,
  useLocation,
  useNavigate,
  useRoutes
} from "react-router-dom";
import Drawer from "@mui/material/Drawer";
import CssBaseline from "@mui/material/CssBaseline";
import { IconButton } from "@components/muiComponents";
import { useSelector } from "react-redux";
import SearchIcon from "@mui/icons-material/Search";
import { CircularProgress, InputBase, Paper, Stack } from "@mui/material";
import { TypeHeaderState } from "@models/treeStructureType.js";
import { globalSessionData, PathAssociateWithModule } from "@utils/Enum";
import KeyboardDoubleArrowLeftIcon from "@mui/icons-material/KeyboardDoubleArrowLeft";
import KeyboardDoubleArrowRightIcon from "@mui/icons-material/KeyboardDoubleArrowRight";
import { useAppDispatch } from "@hooks/reducerHook";
import { fetchEnumData } from "@redux/slice/enumSlice";
import { fetchRootClassification } from "@redux/slice/rootClassificationSlice";
import { fetchTypeHeaderData } from "@redux/slice/typeDefSlices/typeDefHeaderSlice";
import { fetchRootEntity } from "@redux/slice/allEntityTypesSlice";
import { fetchMetricEntity } from "@redux/slice/metricsSlice";
import ErrorPage from "@views/ErrorPage";
import AppRoutes from "@views/AppRoutes";
import ErrorBoundaryWithNavigate from "../../ErrorBoundary";
import useHistory from "@utils/history.js";

const Header = lazy(() => import("@views/Layout/Header"));

const EntitiesTree = lazy(() => import("./SideBarTree/EntitiesTree"));
const ClassificationTree = lazy(
  () => import("./SideBarTree/ClassificationTree")
);
const BusinessMetadataTree = lazy(
  () => import("./SideBarTree/BusinessMetadataTree")
);
const GlossaryTree = lazy(() => import("./SideBarTree/GlossaryTree"));
const RelationshipsTree = lazy(() => import("./SideBarTree/RelationShipsTree"));
const CustomFiltersTree = lazy(() => import("./SideBarTree/CustomFiltersTree"));

export const defaultDrawerWidth = "20%";

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

const DrawerHeader = styled("div")(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  padding: theme.spacing(0, 1),
  ...theme.mixins.toolbar,
  marginBottom: "1rem"
}));

const SideBarBody = (props: {
  loading: boolean;
  handleOpenModal: any;
  handleOpenAboutModal: any;
}) => {
  const location = useLocation();
  const routes = useRoutes(AppRoutes as RouteObject[]);
  const history = useHistory();
  const dispatch = useAppDispatch();
  const { loading: loader, handleOpenModal, handleOpenAboutModal } = props;
  const navigate = useNavigate();
  const { loading } = useSelector((state: TypeHeaderState) => state.typeHeader);
  const { relationshipSearch = {} } = globalSessionData || {};
  const [open, setOpen] = useState(true);
  const [searchTerm, setSearchTerm] = useState<string>("");

  const handleDrawerOpen = () => {
    setOpen(!open);
  };

  const [position, setPosition] = useState<string | number>(defaultDrawerWidth);
  const draggerRef = useRef<HTMLDivElement>(null);
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

  useEffect(() => {
    dispatch(fetchTypeHeaderData());
    dispatch(fetchRootEntity());
    dispatch(fetchRootClassification());
    dispatch(fetchEnumData());
    dispatch(fetchMetricEntity());
  }, []);

  useEffect(() => {
    const draggerElement = draggerRef.current;

    draggerElement?.addEventListener("mousedown", handleMouseDown);

    return () => {
      draggerElement?.removeEventListener("mousedown", handleMouseDown);
    };
  }, []);

  const routeConfig = Object.keys(PathAssociateWithModule).map((key) => {
    return {
      path: PathAssociateWithModule[
        key as keyof typeof PathAssociateWithModule
      ][0],
      element: routes
    };
  });

  const matched = matchRoutes(routeConfig, location.pathname);

  return (
    <Stack
      flexDirection="row"
      className="sidebar-box"
      sx={{ overflow: "hidden" }}
    >
      <CssBaseline />

      <Drawer
        sx={{
          width: position,
          flexShrink: 0,
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
                  className="header-logo cursor-pointer"
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
                    onChange={(e: ChangeEvent<HTMLInputElement>) => {
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
            <Suspense
              fallback={
                <Stack className="tree-item-loader-box">
                  <CircularProgress size="small" className="tree-item-loader" />
                </Stack>
              }
            >
              <EntitiesTree
                sideBarOpen={open}
                loading={loading}
                searchTerm={searchTerm}
              />
            </Suspense>
          </div>

          <div
            className="sidebar-treeview-container"
            data-cy="r_classificationTreeRender"
          >
            <Suspense
              fallback={
                <Stack className="tree-item-loader-box">
                  <CircularProgress size="small" className="tree-item-loader" />
                </Stack>
              }
            >
              <ClassificationTree
                sideBarOpen={open}
                loading={loader}
                searchTerm={searchTerm}
              />
            </Suspense>
          </div>

          <div
            className="sidebar-treeview-container"
            data-cy="r_businessMetadataTreeRender"
          >
            <Suspense
              fallback={
                <Stack className="tree-item-loader-box">
                  <CircularProgress size="small" className="tree-item-loader" />
                </Stack>
              }
            >
              <BusinessMetadataTree
                sideBarOpen={open}
                searchTerm={searchTerm}
              />
            </Suspense>
          </div>

          <div
            className="sidebar-treeview-container"
            data-cy="r_glossaryTreeRender"
          >
            <Suspense
              fallback={
                <Stack className="tree-item-loader-box">
                  <CircularProgress size="small" className="tree-item-loader" />
                </Stack>
              }
            >
              <GlossaryTree sideBarOpen={open} searchTerm={searchTerm} />
            </Suspense>
          </div>
          {relationshipSearch && (
            <div
              className="sidebar-treeview-container"
              data-cy="r_relationshipTreeRender"
            >
              <Suspense
                fallback={
                  <Stack className="tree-item-loader-box">
                    <CircularProgress
                      size="small"
                      className="tree-item-loader"
                    />
                  </Stack>
                }
              >
                <RelationshipsTree sideBarOpen={open} searchTerm={searchTerm} />
              </Suspense>
            </div>
          )}

          <div
            className="sidebar-treeview-container"
            data-cy="r_customFilterTreeRender"
          >
            <Suspense
              fallback={
                <Stack className="tree-item-loader-box">
                  <CircularProgress size="small" className="tree-item-loader" />
                </Stack>
              }
            >
              <CustomFiltersTree sideBarOpen={open} searchTerm={searchTerm} />
            </Suspense>
          </div>
          <div
            style={{
              width: "inherit",
              textAlign: "right",
              padding: "8px",
              position: "fixed",
              bottom: "0px",
              zIndex: "9",
              left: "0",
              background: "#034858"
            }}
          >
            <IconButton size="medium" onClick={() => handleDrawerOpen()}>
              {open ? (
                <KeyboardDoubleArrowLeftIcon
                  sx={{ color: "white" }}
                  fontSize="medium"
                />
              ) : (
                <KeyboardDoubleArrowRightIcon
                  sx={{ color: "white" }}
                  fontSize="medium"
                />
              )}
            </IconButton>
          </div>
        </Paper>
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
            <Suspense fallback={null}>
              <Header
                handleOpenModal={handleOpenModal}
                handleOpenAboutModal={handleOpenAboutModal}
              />
            </Suspense>
          </div>
          <div
            style={{
              padding: "16px",
              display: "flex",
              flex: "1",
              flexDirection: "column"
            }}
          >
            {matched || location.pathname.includes("!") ? (
              <Suspense
                fallback={
                  <div
                    style={{
                      left: 0,
                      top: 0,
                      width: "100%",
                      height: "calc(100vh - 88px)",
                      position: "relative"
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
                <ErrorBoundaryWithNavigate
                  history={history}
                  key={location.pathname}
                >
                  <Outlet />{" "}
                </ErrorBoundaryWithNavigate>
              </Suspense>
            ) : (
              <ErrorPage errorCode="404" />
            )}
          </div>
        </Stack>
      </Main>
    </Stack>
  );
};

export default SideBarBody;
