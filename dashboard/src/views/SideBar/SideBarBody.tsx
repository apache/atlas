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
  useCallback,
  useEffect,
  useState,
  ChangeEvent,
  KeyboardEvent,
  lazy,
  useRef,
  useMemo,
} from "react";
import TreeSkeletonLoader from "@components/TreeSkeletonLoader";
import atlasLogo from "/img/atlas_logo.svg";
import apacheAtlasLogo from "/img/apache-atlas-logo.svg";
import {
  matchRoutes,
  Outlet,
  RouteObject,
  useLocation,
  useNavigate,
  useRoutes,
} from "react-router-dom";
import Drawer from "@mui/material/Drawer";
import CssBaseline from "@mui/material/CssBaseline";
import { IconButton } from "@components/muiComponents";

import ClearIcon from "@mui/icons-material/Clear";
import { getVersion } from "@api/apiMethods/headerApiMethods";
import { InputBase, Paper, Stack, Box, Popover, Typography, Tooltip, CircularProgress } from "@mui/material";
import { globalSessionData, PathAssociateWithModule } from "@utils/Enum";
import KeyboardDoubleArrowLeftIcon from "@mui/icons-material/KeyboardDoubleArrowLeft";
import KeyboardDoubleArrowRightIcon from "@mui/icons-material/KeyboardDoubleArrowRight";
import { useAppDispatch } from "@hooks/reducerHook";
import { fetchEnumData } from "@redux/slice/enumSlice";
import { fetchRootClassification } from "@redux/slice/rootClassificationSlice";
import { fetchTypeHeaderData } from "@redux/slice/typeDefSlices/typeDefHeaderSlice";
import { fetchRootEntity } from "@redux/slice/allEntityTypesSlice";
import { fetchMetricEntity } from "@redux/slice/metricsSlice";
import { refreshDashboardHomeData } from "@utils/refreshDashboardHome";
import ErrorPage from "@views/ErrorPage";
import AppRoutes from "@views/AppRoutes";
import ErrorBoundaryWithNavigate from "../../ErrorBoundary";
import useHistory from "@utils/history.js";
import AccountTreeIcon from "@mui/icons-material/AccountTree";

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
    duration: theme.transitions.duration.leavingScreen,
  }),
  marginLeft: `-${defaultDrawerWidth}`,
  ...(open && {
    transition: theme.transitions.create("margin", {
      easing: theme.transitions.easing.easeOut,
      duration: theme.transitions.duration.enteringScreen,
    }),
    marginLeft: 0,
  }),
}));

const DrawerHeader = styled("div")(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  padding: theme.spacing(0, 1),
  ...theme.mixins.toolbar,
  marginBottom: "1rem",
}));

const SideBarBody = (props: {
  handleOpenModal: any;
  handleOpenAboutModal: any;
}) => {
  const location = useLocation();
  const routes = useRoutes(AppRoutes as RouteObject[]);
  const history = useHistory();
  const dispatch = useAppDispatch();
  const { handleOpenModal, handleOpenAboutModal } = props;
  const navigate = useNavigate();
  const { relationshipSearch = {} } = globalSessionData || {};
  const [open, setOpen] = useState(true);
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [versionData, setVersionData] = useState<any>({});
  const searchParams = new URLSearchParams(location.search);

  const isCustomFilterActive = searchParams.get("isCF") === "true";
  const isGlossaryActive = !isCustomFilterActive && (location.pathname.includes("/glossary") || !!searchParams.get("gtype") || !!searchParams.get("term") || !!searchParams.get("category"));
  const isBusinessMetadataActive = !isCustomFilterActive && location.pathname.includes("/administrator/businessMetadata");
  const isClassificationActive = !isCustomFilterActive && (!!searchParams.get("tag") || location.pathname.includes("/tag/tagAttribute"));
  const isRelationshipActive = !isCustomFilterActive && (!!searchParams.get("relationshipName") || location.pathname.includes("/relationshipDetailPage"));

  const isEntitiesActive = !isCustomFilterActive && (!!searchParams.get("type") || location.pathname.includes("/detailPage"));

  const handleDrawerOpen = () => {
    setOpen(!open);
  };

  const [popoverAnchor, setPopoverAnchor] = useState<HTMLElement | null>(null);
  const [activePopover, setActivePopover] = useState<string | null>(null);

  const handlePopoverOpen = (event: React.MouseEvent<HTMLElement>, id: string) => {
    setPopoverAnchor(event.currentTarget);
    setActivePopover(id);
  };

  const handlePopoverClose = () => {
    setPopoverAnchor(null);
    setActivePopover(null);
  };



  const renderPopoverSearch = () => (
    <div style={{ padding: "8px", borderBottom: "1px solid rgba(255,255,255,0.1)", marginBottom: "4px" }}>
      <Paper className="sidebar-searchbar" sx={{ width: "100%", display: "flex", alignItems: "center", paddingLeft: "8px" }}>
        <InputBase
          fullWidth
          sx={{ color: "rgba(0, 0, 0, 0.7)" }}
          placeholder="Search"
          value={searchTerm}
          onChange={(e: ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)}
          endAdornment={
            <Stack direction="row" alignItems="center" gap="4px">
              {searchTerm.length > 0 && (
                <IconButton
                  size="small"
                  onClick={() => setSearchTerm("")}
                  edge="end"
                  sx={{ padding: "4px" }}
                >
                  <ClearIcon fontSize="small" sx={{ color: "rgba(0, 0, 0, 0.4)" }} />
                </IconButton>
              )}
              <img src="/img/sidebar-icons/icon-search.svg" style={{ width: "16px", height: "16px", filter: "brightness(0.4)", opacity: 1, cursor: "pointer", marginLeft: "4px" }} alt="Search" />
            </Stack>
          }
        />
      </Paper>
    </div>
  );

  const [position, setPosition] = useState<string | number>(defaultDrawerWidth);
  const draggerRef = useRef<HTMLDivElement>(null);
  const headerRef = useRef<HTMLDivElement>(null);
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

    // Fetch version data for footer
    const fetchVersion = async () => {
      try {
        const resp = await getVersion();
        if (resp?.data) setVersionData(resp.data);
      } catch (e) {
        console.error(e);
      }
    };
    fetchVersion();
  }, [dispatch]);

  const handleAtlasLogoClick = useCallback(() => {
    refreshDashboardHomeData(dispatch);
    navigate(
      {
        pathname: "/search",
      },
      { replace: true }
    );
  }, [dispatch, navigate]);

  const handleAtlasLogoKeyDown = useCallback(
    (e: KeyboardEvent<HTMLElement>) => {
      if (e.key !== "Enter" && e.key !== " ") {
        return;
      }
      e.preventDefault();
      handleAtlasLogoClick();
    },
    [handleAtlasLogoClick]
  );

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
      element: routes,
    };
  });

  const matched = matchRoutes(routeConfig, location.pathname);
  const isMatched = !!matched;

  const rightSideContent = useMemo(() => (
    <Stack height="auto" minHeight="100%">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          backgroundColor: "white",
          height: "56px",
          alignItems: "center",
          padding: "16px",
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
          flexDirection: "column",
        }}
      >
        {isMatched || location.pathname.includes("!") ? (
          <Suspense
            fallback={
              <div
                style={{
                  left: 0,
                  top: 0,
                  width: "100%",
                  height: "calc(100vh - 88px)",
                  position: "relative",
                }}
              >
                <CircularProgress
                  color="primary"
                  sx={{
                    display: "inline-block",
                    position: "absolute",
                    left: "50%",
                    top: "50%",
                    transform: "translate(-50%, -50%)",
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
  ), [isMatched, location.pathname, history, handleOpenModal, handleOpenAboutModal]);

  return (
    <Stack
      flexDirection="row"
      className="sidebar-box"
      sx={{ overflow: "hidden" }}
    >
      <CssBaseline />

      <Drawer
        sx={{
          width: open ? position : "60px",
          flexShrink: 0,
          minHeight: "calc(100vh - 64px)",
          minWidth: "60px",
          transition: "width 0.2s",
          ...(!open && {
            transform: "none !important",
            visibility: "visible !important",
          }),
          "& .MuiDrawer-paper": {
            background: "#034858",
            boxSizing: "border-box",
            overflow: "hidden",
            position: "fixed",
            top: "0",
            left: "0",
            width: open ? position : "60px",
            transition: "width 0.2s",
            ...(!open && {
              transform: "none !important",
              visibility: "visible !important",
            }),
          },
        }}
        PaperProps={{
          style: { width: open ? position : "60px", minWidth: "60px" },
        }}
        variant="persistent"
        anchor="left"
        open={open}
      >
        <Stack
          sx={{
            height: "100vh",
            width: "100%",
            backgroundColor: "#034858",
          }}
        >
          {/* Collapsed sidebar logo and module icons */}
          {!open && (
            <Stack
              alignItems="center"
              sx={{ width: "100%", flex: 1, minHeight: 0, overflowY: "auto", overflowX: "hidden", boxSizing: "border-box", pb: 2, '&::-webkit-scrollbar': { display: 'none' }, msOverflowStyle: 'none', scrollbarWidth: 'none' }}
            >
              <div
                style={{
                  width: "100%",
                  textAlign: "center",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  minHeight: "64px",
                  cursor: "pointer",
                  boxSizing: "border-box",
                  marginBottom: "1rem",
                }}
                role="button"
                tabIndex={0}
                aria-label="Atlas home — refresh dashboard"
                onClick={handleAtlasLogoClick}
                onKeyDown={handleAtlasLogoKeyDown}
                data-cy="apache-atlas-logo-collapsed"
              >
                <img
                  src={apacheAtlasLogo}
                  alt="Apache Atlas logo"
                  style={{
                    width: "29px",
                    height: "auto",
                    maxWidth: "100%",
                    display: "block",
                  }}
                />
              </div>

              {/* Module Icons for Mini Drawer */}
              <Stack alignItems="stretch" gap="1rem" sx={{ width: "100%" }}>
                {/* Search */}
                <Box sx={{ display: "flex", justifyContent: "center", borderLeft: "4px solid transparent", borderRight: "4px solid transparent", background: "transparent" }}>
                  <Tooltip title="Search" placement="right">
                    <IconButton onClick={() => setOpen(true)} sx={{ '&:hover': { background: 'rgba(255, 255, 255, 0.1)' } }}>
                      <img src="/img/sidebar-icons/icon-search.svg" style={{ width: "20px", height: "20px", opacity: 1 }} alt="search" />
                    </IconButton>
                  </Tooltip>
                </Box>

                {/* Entities */}
                <Box sx={{ display: "flex", justifyContent: "center", borderLeft: isEntitiesActive ? "4px solid #2ccebb" : "4px solid transparent", borderRight: "4px solid transparent", background: isEntitiesActive ? "rgba(255, 255, 255, 0.08)" : "transparent" }}>
                  <Tooltip title="Entities" placement="right">
                    <IconButton onClick={(e) => handlePopoverOpen(e, "entities")} sx={{ '&:hover': { background: 'rgba(255, 255, 255, 0.1)' } }}>
                      <img src="/img/sidebar-icons/icon-entities.svg" style={{ width: "20px", height: "20px", opacity: 1 }} alt="entities" />
                    </IconButton>
                  </Tooltip>
                </Box>
                <Popover
                  marginThreshold={64}
                  open={activePopover === "entities"}
                  anchorEl={popoverAnchor}
                  onClose={handlePopoverClose}
                  anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
                  transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                  PaperProps={{ sx: { ml: 1, width: 320, maxHeight: 'calc(100vh - 250px) !important', display: 'flex', flexDirection: 'column', backgroundColor: '#034858', borderRadius: 1, boxShadow: 6, pb: 2, overflow: 'visible', '&::before': { content: '""', display: 'block', position: 'absolute', top: 14, left: -8, width: 0, height: 0, borderTop: '8px solid transparent', borderBottom: '8px solid transparent', borderRight: '8px solid #034858' } } }}
                >
                  {renderPopoverSearch()}
                  <div style={{ flex: 1, overflow: 'auto' }}>
                    <Suspense fallback={<TreeSkeletonLoader count={2} />}>
                      <div className="sidebar-treeview-container" style={{ padding: '8px' }}>
                        <EntitiesTree sideBarOpen={true} searchTerm={searchTerm} isPopover={true} />
                      </div>
                    </Suspense>

                  </div>
                </Popover>

                {/* Classifications */}
                <Box sx={{ display: "flex", justifyContent: "center", borderLeft: isClassificationActive ? "4px solid #2ccebb" : "4px solid transparent", borderRight: "4px solid transparent", background: isClassificationActive ? "rgba(255, 255, 255, 0.08)" : "transparent" }}>
                  <Tooltip title="Classifications" placement="right">
                    <IconButton onClick={(e) => handlePopoverOpen(e, "classification")} sx={{ '&:hover': { background: 'rgba(255, 255, 255, 0.1)' } }}>
                      <img src="/img/sidebar-icons/icon-classifications.svg" style={{ width: "20px", height: "20px", opacity: 1 }} alt="classifications" />
                    </IconButton>
                  </Tooltip>
                </Box>
                <Popover
                  marginThreshold={64}
                  open={activePopover === "classification"}
                  anchorEl={popoverAnchor}
                  onClose={handlePopoverClose}
                  anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
                  transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                  PaperProps={{ sx: { ml: 1, width: 320, maxHeight: 'calc(100vh - 250px) !important', display: 'flex', flexDirection: 'column', backgroundColor: '#034858', borderRadius: 1, boxShadow: 6, pb: 2, overflow: 'visible', '&::before': { content: '""', display: 'block', position: 'absolute', top: 14, left: -8, width: 0, height: 0, borderTop: '8px solid transparent', borderBottom: '8px solid transparent', borderRight: '8px solid #034858' } } }}
                >
                  {renderPopoverSearch()}
                  <div style={{ flex: 1, overflow: 'auto' }}>
                    <Suspense fallback={<TreeSkeletonLoader count={2} />}>
                      <div className="sidebar-treeview-container" style={{ padding: '8px' }}>
                        <ClassificationTree sideBarOpen={true} searchTerm={searchTerm} isPopover={true} />
                      </div>
                    </Suspense>

                  </div>
                </Popover>

                {/* Glossary */}
                <Box sx={{ display: "flex", justifyContent: "center", borderLeft: isGlossaryActive ? "4px solid #2ccebb" : "4px solid transparent", borderRight: "4px solid transparent", background: isGlossaryActive ? "rgba(255, 255, 255, 0.08)" : "transparent" }}>
                  <Tooltip title="Glossary" placement="right">
                    <IconButton onClick={(e) => handlePopoverOpen(e, "glossary")} sx={{ '&:hover': { background: 'rgba(255, 255, 255, 0.1)' } }}>
                      <img src="/img/sidebar-icons/icon-glossary.svg" style={{ width: "20px", height: "20px", opacity: 1 }} alt="glossary" />
                    </IconButton>
                  </Tooltip>
                </Box>
                <Popover
                  marginThreshold={64}
                  open={activePopover === "glossary"}
                  anchorEl={popoverAnchor}
                  onClose={handlePopoverClose}
                  anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
                  transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                  PaperProps={{ sx: { ml: 1, width: 320, maxHeight: 'calc(100vh - 250px) !important', display: 'flex', flexDirection: 'column', backgroundColor: '#034858', borderRadius: 1, boxShadow: 6, pb: 2, overflow: 'visible', '&::before': { content: '""', display: 'block', position: 'absolute', top: 14, left: -8, width: 0, height: 0, borderTop: '8px solid transparent', borderBottom: '8px solid transparent', borderRight: '8px solid #034858' } } }}
                >
                  {renderPopoverSearch()}
                  <div style={{ flex: 1, overflow: 'auto' }}>
                    <Suspense fallback={<TreeSkeletonLoader count={2} />}>
                      <div className="sidebar-treeview-container" style={{ padding: '8px' }}>
                        <GlossaryTree sideBarOpen={true} searchTerm={searchTerm} isPopover={true} />
                      </div>
                    </Suspense>

                  </div>
                </Popover>

                {/* Business Metadata */}
                <Box sx={{ display: "flex", justifyContent: "center", borderLeft: isBusinessMetadataActive ? "4px solid #2ccebb" : "4px solid transparent", borderRight: "4px solid transparent", background: isBusinessMetadataActive ? "rgba(255, 255, 255, 0.08)" : "transparent" }}>
                  <Tooltip title="Business Metadata" placement="right">
                    <IconButton onClick={(e) => handlePopoverOpen(e, "businessMetadata")} sx={{ '&:hover': { background: 'rgba(255, 255, 255, 0.1)' } }}>
                      <img src="/img/sidebar-icons/icon-business-metadata.svg" style={{ width: "20px", height: "20px", opacity: 1 }} alt="business metadata" />
                    </IconButton>
                  </Tooltip>
                </Box>
                <Popover
                  marginThreshold={64}
                  open={activePopover === "businessMetadata"}
                  anchorEl={popoverAnchor}
                  onClose={handlePopoverClose}
                  anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
                  transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                  PaperProps={{ sx: { ml: 1, width: 320, maxHeight: 'calc(100vh - 250px) !important', display: 'flex', flexDirection: 'column', backgroundColor: '#034858', borderRadius: 1, boxShadow: 6, pb: 2, overflow: 'visible', '&::before': { content: '""', display: 'block', position: 'absolute', top: 14, left: -8, width: 0, height: 0, borderTop: '8px solid transparent', borderBottom: '8px solid transparent', borderRight: '8px solid #034858' } } }}
                >
                  {renderPopoverSearch()}
                  <div style={{ flex: 1, overflow: 'auto' }}>
                    <Suspense fallback={<TreeSkeletonLoader count={2} />}>
                      <div className="sidebar-treeview-container" style={{ padding: '8px' }}>
                        <BusinessMetadataTree sideBarOpen={true} searchTerm={searchTerm} isPopover={true} />
                      </div>
                    </Suspense>

                  </div>
                </Popover>

                {/* Relationships */}
                {relationshipSearch && (
                  <>
                    <Box sx={{ display: "flex", justifyContent: "center", borderLeft: isRelationshipActive ? "4px solid #2ccebb" : "4px solid transparent", background: isRelationshipActive ? "rgba(255, 255, 255, 0.08)" : "transparent" }}>
                      <Tooltip title="Relationships" placement="right">
                        <IconButton onClick={(e) => handlePopoverOpen(e, "relationships")} sx={{ color: isRelationshipActive ? "white" : "rgba(255, 255, 255, 0.6)", '&:hover': { color: 'white', background: 'rgba(255, 255, 255, 0.1)' } }}>
                          <AccountTreeIcon />
                        </IconButton>
                      </Tooltip>
                    </Box>
                    <Popover
                      marginThreshold={64}
                      open={activePopover === "relationships"}
                      anchorEl={popoverAnchor}
                      onClose={handlePopoverClose}
                      anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
                      transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                      PaperProps={{ sx: { ml: 1, width: 320, maxHeight: 'calc(100vh - 250px) !important', display: 'flex', flexDirection: 'column', backgroundColor: '#034858', borderRadius: 1, boxShadow: 6, pb: 2, overflow: 'visible', '&::before': { content: '""', display: 'block', position: 'absolute', top: 14, left: -8, width: 0, height: 0, borderTop: '8px solid transparent', borderBottom: '8px solid transparent', borderRight: '8px solid #034858' } } }}
                    >
                      {renderPopoverSearch()}
                      <div style={{ flex: 1, overflow: 'auto' }}>
                        <Suspense fallback={<TreeSkeletonLoader count={2} />}>
                          <div className="sidebar-treeview-container" style={{ padding: '8px' }}>
                            <RelationshipsTree sideBarOpen={true} searchTerm={searchTerm} isPopover={true} />
                          </div>
                        </Suspense>

                      </div>
                    </Popover>
                  </>
                )}

                {/* Custom Filters */}
                <Box sx={{ display: "flex", justifyContent: "center", borderLeft: isCustomFilterActive ? "4px solid #2ccebb" : "4px solid transparent", borderRight: "4px solid transparent", background: isCustomFilterActive ? "rgba(255, 255, 255, 0.08)" : "transparent" }}>
                  <Tooltip title="Custom Filters" placement="right">
                    <IconButton onClick={(e) => handlePopoverOpen(e, "customFilters")} sx={{ '&:hover': { background: 'rgba(255, 255, 255, 0.1)' } }}>
                      <img src="/img/sidebar-icons/icon-custom-filters.svg" style={{ width: "20px", height: "20px", opacity: 1 }} alt="custom filters" />
                    </IconButton>
                  </Tooltip>
                </Box>
                <Popover
                  marginThreshold={64}
                  open={activePopover === "customFilters"}
                  anchorEl={popoverAnchor}
                  onClose={handlePopoverClose}
                  anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
                  transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                  PaperProps={{ sx: { ml: 1, width: 320, maxHeight: 'calc(100vh - 250px) !important', display: 'flex', flexDirection: 'column', backgroundColor: '#034858', borderRadius: 1, boxShadow: 6, pb: 2, overflow: 'visible', '&::before': { content: '""', display: 'block', position: 'absolute', top: 14, left: -8, width: 0, height: 0, borderTop: '8px solid transparent', borderBottom: '8px solid transparent', borderRight: '8px solid #034858' } } }}
                >
                  {renderPopoverSearch()}
                  <div style={{ flex: 1, overflow: 'auto' }}>
                    <Suspense fallback={<TreeSkeletonLoader count={2} />}>
                      <div className="sidebar-treeview-container" style={{ padding: '8px' }}>
                        <CustomFiltersTree sideBarOpen={true} searchTerm={searchTerm} isPopover={true} />
                      </div>
                    </Suspense>

                  </div>
                </Popover>
              </Stack>
            </Stack>
          )}

          {open && (
            <DrawerHeader
              ref={headerRef}
              sx={{
                position: "sticky",
                top: 0,
                zIndex: 10,
                backgroundColor: "#034858",
                flexShrink: 0,
              }}
            >
              <Stack gap="1.5rem" width="100%" marginTop="1rem">
                <span
                  role="button"
                  tabIndex={0}
                  aria-label="Atlas home — refresh dashboard"
                  onClick={handleAtlasLogoClick}
                  onKeyDown={handleAtlasLogoKeyDown}
                  className="inline-block cursor-pointer"
                >
                  <img
                    src={atlasLogo}
                    alt=""
                    aria-hidden
                    className="header-logo"
                    data-cy="atlas-logo"
                  />
                </span>
                <Paper
                  sx={{
                    width: "100%",
                    paddingLeft: "8px"
                  }}
                  className="sidebar-searchbar"
                >
                  <InputBase
                    fullWidth
                    sx={{ color: "rgba(0, 0, 0, 0.7)" }}
                    placeholder="Search"
                    inputProps={{ "aria-label": "search" }}
                    value={searchTerm}
                    onChange={(e: ChangeEvent<HTMLInputElement>) => {
                      setSearchTerm(e.target.value);
                    }}
                    data-cy="searchNode"
                    endAdornment={
                      <Stack direction="row" alignItems="center" gap="4px">
                        {searchTerm.length > 0 && (
                          <IconButton
                            size="small"
                            onClick={() => setSearchTerm("")}
                            edge="end"
                            sx={{ padding: "4px" }}
                          >
                            <ClearIcon fontSize="small" sx={{ color: "rgba(0, 0, 0, 0.4)" }} />
                          </IconButton>
                        )}
                        <img src="/img/sidebar-icons/icon-search.svg" style={{ width: "16px", height: "16px", filter: "brightness(0.4)", opacity: 1, cursor: "pointer", marginLeft: "4px" }} alt="Search" />
                      </Stack>
                    }
                  />
                </Paper>
              </Stack>
            </DrawerHeader>
          )}
          <Paper
            className="sidebar-wrapper"
            sx={{
              flex: 1,
              overflowX: "hidden",
              overflowY: "auto",
              paddingBottom: "48px", // Added space so it doesn't touch the bottom toggle button
              ...(open == false && {
                overflow: "hidden",
                display: "none",
              }),
            }}
          >
            <div
              className="sidebar-treeview-container"
              data-cy="r_entityTreeRender"
            >
              <Suspense
                fallback={<TreeSkeletonLoader count={2} />}
              >
                <EntitiesTree
                  sideBarOpen={open}
                  searchTerm={searchTerm}
                />
              </Suspense>
            </div>

            <div
              className="sidebar-treeview-container"
              data-cy="r_classificationTreeRender"
            >
              <Suspense
                fallback={<TreeSkeletonLoader count={2} />}
              >
                <ClassificationTree
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
                fallback={<TreeSkeletonLoader count={2} />}
              >
                <GlossaryTree sideBarOpen={open} searchTerm={searchTerm} />
              </Suspense>
            </div>

            <div
              className="sidebar-treeview-container"
              data-cy="r_businessMetadataTreeRender"
            >
              <Suspense
                fallback={<TreeSkeletonLoader count={2} />}
              >
                <BusinessMetadataTree
                  sideBarOpen={open}
                  searchTerm={searchTerm}
                />
              </Suspense>
            </div>
            {relationshipSearch && (
              <div
                className="sidebar-treeview-container"
                data-cy="r_relationshipTreeRender"
              >
                <Suspense
                  fallback={<TreeSkeletonLoader count={2} />}
                >
                  <RelationshipsTree
                    sideBarOpen={open}
                    searchTerm={searchTerm}
                  />
                </Suspense>
              </div>
            )}

            <div
              className="sidebar-treeview-container"
              data-cy="r_customFilterTreeRender"
            >
              <Suspense
                fallback={<TreeSkeletonLoader count={2} />}
              >
                <CustomFiltersTree sideBarOpen={open} searchTerm={searchTerm} />
              </Suspense>
            </div>
          </Paper>
          <div
            style={{
              width: "100%",
              padding: "8px",
              position: "sticky",
              bottom: "0px",
              zIndex: "9",
              left: "0",
              background: "#034858",
              display: "flex",
              flexDirection: open ? "row" : "column",
              justifyContent: open ? "space-between" : "center",
              alignItems: "center",
              gap: open ? "0px" : "4px"
            }}
          >
            {open && (
              <Box display="flex" flexDirection="column" gap="4px" alignItems="flex-start" pl="4px">
                <Typography variant="body2" sx={{ color: "rgba(255, 255, 255, 0.6)", pl: '4px' }}>
                  V {versionData?.Version || '3.12.1.0'}
                </Typography>
              </Box>
            )}

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
        </Stack>
      </Drawer>

      <Main
        open={open}
        sx={{
          margin: "0",
          overflowX: "auto",
          background: "#f5f7f9",
          padding: "0",
        }}
      >
        {rightSideContent}
      </Main>
    </Stack>
  );
};

export default SideBarBody;
