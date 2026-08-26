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
  KeyboardEvent,
  lazy,
  useRef,
  useMemo,
} from "react";
import TreeSkeletonLoader from "@components/TreeSkeletonLoader";
import { SidebarSearchInput } from "@components/SidebarSearchInput";
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

import { Paper, Stack, Box, Popover, Typography, Tooltip, CircularProgress } from "@mui/material";
import { globalSessionData, PathAssociateWithModule } from "@utils/Enum";
import KeyboardDoubleArrowLeftIcon from "@mui/icons-material/KeyboardDoubleArrowLeft";
import KeyboardDoubleArrowRightIcon from "@mui/icons-material/KeyboardDoubleArrowRight";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchEnumData } from "@redux/slice/enumSlice";
import { fetchRootClassification } from "@redux/slice/rootClassificationSlice";
import { fetchTypeHeaderData } from "@redux/slice/typeDefSlices/typeDefHeaderSlice";
import { fetchRootEntity } from "@redux/slice/allEntityTypesSlice";
import { fetchMetricEntity } from "@redux/slice/metricsSlice";
import { fetchVersionData } from "@redux/slice/sessionSlice";
import { refreshDashboardHomeData } from "@utils/refreshDashboardHome";
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
  handleOpenModal: () => void;
  handleOpenAboutModal: () => void;
}) => {
  const location = useLocation();
  const routes = useRoutes(AppRoutes as RouteObject[]);
  const history = useHistory();
  const dispatch = useAppDispatch();
  const { handleOpenModal, handleOpenAboutModal } = props;
  const navigate = useNavigate();
  const { relationshipSearch = false } = globalSessionData || {};
  const [open, setOpen] = useState(true);
  const [searchTerm, setSearchTerm] = useState<string>("");
  const { data: versionData, loading: isVersionLoading, error: versionError } = useAppSelector((state) => state.session?.versionData || {});
  const activeModule = useMemo(() => {
    const searchParams = new URLSearchParams(location.search);
    if (searchParams.get("isCF") === "true") return "customFilters";
    if (location.pathname.includes("/glossary") || !!searchParams.get("gtype") || !!searchParams.get("term") || !!searchParams.get("category")) return "glossary";
    if (location.pathname.includes("/administrator/businessMetadata")) return "businessMetadata";
    if (!!searchParams.get("tag") || location.pathname.includes("/tag/tagAttribute")) return "classification";
    if (!!searchParams.get("relationshipName") || location.pathname.includes("/relationshipDetailPage")) return "relationships";
    if (!!searchParams.get("type") || location.pathname.includes("/detailPage")) return "entities";
    return null;
  }, [location.pathname, location.search]);

  const isCustomFilterActive = activeModule === "customFilters";
  const isGlossaryActive = activeModule === "glossary";
  const isBusinessMetadataActive = activeModule === "businessMetadata";
  const isClassificationActive = activeModule === "classification";
  const isRelationshipActive = activeModule === "relationships";
  const isEntitiesActive = activeModule === "entities";

  const modules = useMemo(() => [
    { id: "entities", title: "Entities", isActive: isEntitiesActive, iconUrl: "/img/sidebar-icons/icon-entities.svg", Component: EntitiesTree, isVisible: true },
    { id: "classification", title: "Classifications", isActive: isClassificationActive, iconUrl: "/img/sidebar-icons/icon-classifications.svg", Component: ClassificationTree, isVisible: true },
    { id: "glossary", title: "Glossary", isActive: isGlossaryActive, iconUrl: "/img/sidebar-icons/icon-glossary.svg", Component: GlossaryTree, isVisible: true },
    { id: "businessMetadata", title: "Business Metadata", isActive: isBusinessMetadataActive, iconUrl: "/img/sidebar-icons/icon-business-metadata.svg", Component: BusinessMetadataTree, isVisible: true },
    { id: "relationships", title: "Relationships", isActive: isRelationshipActive, iconUrl: "/img/sidebar-icons/icon-relationships.svg", Component: RelationshipsTree, isVisible: !!relationshipSearch },
    { id: "customFilters", title: "Custom Filters", isActive: isCustomFilterActive, iconUrl: "/img/sidebar-icons/icon-custom-filters.svg", Component: CustomFiltersTree, isVisible: true }
  ], [
    isEntitiesActive,
    isClassificationActive,
    isGlossaryActive,
    isBusinessMetadataActive,
    isRelationshipActive,
    isCustomFilterActive,
    relationshipSearch
  ]);

  const [popoverAnchor, setPopoverAnchor] = useState<HTMLButtonElement | null>(null);
  const [activePopover, setActivePopover] = useState<string | null>(null);
  const [popoverMaxHeight, setPopoverMaxHeight] = useState<string>("calc(100vh - 100px)");
  const [isBottomHalf, setIsBottomHalf] = useState<boolean>(false);


  const handlePopoverOpen = (event: React.MouseEvent<HTMLButtonElement>, id: string) => {
    const target = event.currentTarget;

    const openNewPopover = () => {
      setPopoverAnchor(target);
      setActivePopover(id);

      // Calculate remaining screen height from the anchor to the bottom
      const rect = target.getBoundingClientRect();
      const spaceBelow = window.innerHeight - rect.top - 24;
      const isBottom = spaceBelow < 350;
      setIsBottomHalf(isBottom);

      if (isBottom) {
        const spaceAbove = rect.bottom - 24;
        setPopoverMaxHeight(`${Math.max(250, spaceAbove)}px`);
      } else {
        setPopoverMaxHeight(`${Math.max(250, spaceBelow)}px`);
      }
    };

    openNewPopover();
  };

  const handlePopoverClose = () => {
    setPopoverAnchor(null);
    setActivePopover(null);
  };

  const handleDrawerOpen = () => {
    setOpen(!open);
    if (!open) {
      handlePopoverClose();
    }
  };



  const renderPopoverSearch = () => (
    <div className="sidebar-popover-search">
      <SidebarSearchInput searchTerm={searchTerm} onChange={setSearchTerm} />
    </div>
  );

  const headerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    dispatch(fetchTypeHeaderData());
    dispatch(fetchRootEntity());
    dispatch(fetchRootClassification());
    dispatch(fetchEnumData());
    dispatch(fetchMetricEntity());
    dispatch(fetchVersionData());
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
      <div className="layout-header-container">
        <Suspense fallback={null}>
          <Header
            handleOpenModal={handleOpenModal}
            handleOpenAboutModal={handleOpenAboutModal}
          />
        </Suspense>
      </div>
      <div className="layout-content-container">
        {isMatched || location.pathname.includes("!") ? (
          <Suspense
            fallback={
              <div className="layout-loading-container">
                <CircularProgress
                  color="primary"
                  className="sidebar-circular-progress"
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
    >
      <CssBaseline />

      <Drawer
        className={`sidebar-drawer ${open ? "open" : "closed"}`}
        PaperProps={{
          className: "sidebar-drawer-paper"
        }}
        variant="persistent"
        anchor="left"
        open={open}
      >
        <Stack className="sidebar-stack">
          {/* Collapsed sidebar logo and module icons */}
          {!open && (
            <Stack
              alignItems="center"
              className="sidebar-mini-module-container"
            >
              <div
                className="collapsed-logo-container"
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
                  className="collapsed-logo-img"
                />
              </div>

              {/* Module Icons for Mini Drawer */}
              <Stack alignItems="stretch" gap="1rem" className="sidebar-module-stack">
                {/* Search */}
                <Box className="sidebar-module-box">
                  <Tooltip title="Search" placement="right">
                    <IconButton aria-label="Expand sidebar search" onClick={() => { setOpen(true); handlePopoverClose(); }} className="sidebar-module-btn">
                      <img src="/img/sidebar-icons/icon-search.svg" className="sidebar-module-icon" alt="search" />
                    </IconButton>
                  </Tooltip>
                </Box>

                {modules.filter(m => m.isVisible).map(m => (
                  <Box
                    key={m.id}
                    className={`sidebar-module-box ${m.isActive ? "sidebar-icon-active" : ""}`}
                  >
                    <Tooltip title={m.title} placement="right">
                      <IconButton aria-haspopup="dialog" aria-label={m.title} aria-expanded={activePopover === m.id} onClick={(e) => handlePopoverOpen(e, m.id)} className={`sidebar-module-btn ${m.isActive ? "active" : ""}`}>
                        <img src={m.iconUrl} className="sidebar-module-icon" alt={m.title.toLowerCase()} />
                      </IconButton>
                    </Tooltip>
                  </Box>
                ))}
              </Stack>

              <Popover
                marginThreshold={16}
                open={Boolean(activePopover) && activePopover !== ""}
                anchorEl={popoverAnchor}
                onClose={handlePopoverClose}
                anchorOrigin={{
                  vertical: isBottomHalf ? "bottom" : "top",
                  horizontal: "right"
                }}
                transformOrigin={{
                  vertical: isBottomHalf ? "bottom" : "top",
                  horizontal: "left"
                }}
                PaperProps={{
                  className: `sidebar-popover-paper ${isBottomHalf ? "bottom-half" : "top-half"}`,
                  style: { maxHeight: popoverMaxHeight }
                }}
              >
                {renderPopoverSearch()}
                <div className="sidebar-module-icon-container">
                  <Suspense fallback={<TreeSkeletonLoader count={2} />}>
                    <div className="sidebar-treeview-container sidebar-toolbar">
                      {modules.filter(m => m.isVisible && activePopover === m.id).map(m => {
                        const Component = m.Component;
                        return <Component key={m.id} sideBarOpen={true} searchTerm={searchTerm} isPopover={true} />;
                      })}
                    </div>
                  </Suspense>
                </div>
              </Popover>
            </Stack>
          )}

          {open && (
            <DrawerHeader
              ref={headerRef}
              className="sidebar-drawer-header"
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
                <SidebarSearchInput
                  searchTerm={searchTerm}
                  onChange={setSearchTerm}
                  dataCy="searchNode"
                />
              </Stack>
            </DrawerHeader>
          )}
          <Paper
            className="sidebar-wrapper"
            style={{ display: open ? "block" : "none" }}
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
            className={`sidebar-toggle-container ${open ? "sidebar-toggle-open" : "sidebar-toggle-closed"}`}
          >
            {open && (
              <div className="sidebar-version-container">
                <Typography variant="body2" className="sidebar-version-text">
                  {isVersionLoading ? (
                    <CircularProgress size={12} className="sidebar-version-loader" />
                  ) : versionError ? (
                    'Version unavailable'
                  ) : versionData?.Version ? (
                    `V ${versionData.Version}`
                  ) : (
                    ''
                  )}
                </Typography>
              </div>
            )}

            <IconButton aria-label={open ? "Collapse sidebar" : "Expand sidebar"} size="medium" onClick={() => handleDrawerOpen()}>
              {open ? (
                <KeyboardDoubleArrowLeftIcon
                  className="sidebar-toggle-icon"
                  fontSize="medium"
                />
              ) : (
                <KeyboardDoubleArrowRightIcon
                  className="sidebar-toggle-icon"
                  fontSize="medium"
                />
              )}
            </IconButton>
          </div>
        </Stack>
      </Drawer>

      <Main
        open={open}
        className="sidebar-main-content"
      >
        {rightSideContent}
      </Main>
    </Stack>
  );
};

export default SideBarBody;
