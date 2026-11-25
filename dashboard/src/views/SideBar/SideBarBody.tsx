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
import iconEntities from "/img/sidebar-icons/icon-entities.svg";
import iconClassifications from "/img/sidebar-icons/icon-classifications.svg";
import iconGlossary from "/img/sidebar-icons/icon-glossary.svg";
import iconBusinessMetadata from "/img/sidebar-icons/icon-business-metadata.svg";
import iconRelationships from "/img/sidebar-icons/icon-relationships.svg";
import iconCustomFilters from "/img/sidebar-icons/icon-custom-filters.svg";
import iconSearch from "/img/sidebar-icons/icon-search.svg";
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
import { refreshDashboardHomeData } from "@utils/refreshDashboardHome";
import SidebarTreePortal from "./SidebarTreePortal";
import { buildSidebarTreeModules } from "./sidebarTreeConfig";
import { useSidebarTypedefBootstrap } from "./useSidebarTypedefBootstrap";
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
const RelationshipsTree = lazy(() => import("./SideBarTree/RelationshipsTree"));
const CustomFiltersTree = lazy(() => import("./SideBarTree/CustomFiltersTree"));

export const defaultDrawerWidth = "20%";

const Main = styled("main")(({ theme }) => ({
  flexGrow: 1,
  minWidth: 0,
  padding: 0,
  transition: theme.transitions.create(["margin", "width"], {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
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
  useSidebarTypedefBootstrap();
  const relationshipSearch = Boolean(globalSessionData?.relationshipSearch);
  const [open, setOpen] = useState(true);
  const [searchTerm, setSearchTerm] = useState<string>("");
  const { data: versionData, loading: isVersionLoading, error: versionError } = useAppSelector((state) => state.session?.versionData || {});
  const { isCustomFilterActive, isGlossaryActive, isBusinessMetadataActive, isClassificationActive, isRelationshipActive, isEntitiesActive } = useMemo(() => {
    const searchParams = new URLSearchParams(location.search);
    const isCustomFilterActive = searchParams.get("isCF") === "true";
    const isBusinessMetadataActive = location.pathname.includes("/administrator/businessMetadata");
    const isRelationshipActive = !!searchParams.get("relationshipName") || location.pathname.includes("/relationshipDetailPage");

    // Entity, Classification, and Glossary can be active simultaneously (e.g. basic search),
    // but not if Custom Filters, Business Metadata, or Relationships is the active module.
    const isExclusiveModuleActive = isCustomFilterActive || isBusinessMetadataActive || isRelationshipActive;

    const isGlossaryActive = !isExclusiveModuleActive && (location.pathname.includes("/glossary") || !!searchParams.get("gtype") || !!searchParams.get("term") || !!searchParams.get("category"));
    const isClassificationActive = !isExclusiveModuleActive && (!!searchParams.get("tag") || location.pathname.includes("/tag/tagAttribute"));
    const isEntitiesActive = !isExclusiveModuleActive && (!!searchParams.get("type") || location.pathname.includes("/detailPage"));

    return {
      isCustomFilterActive,
      isGlossaryActive,
      isBusinessMetadataActive,
      isClassificationActive,
      isRelationshipActive,
      isEntitiesActive
    };
  }, [location.pathname, location.search]);

  const modules = useMemo(() => [
    { id: "entities", title: "Entities", isActive: isEntitiesActive, iconUrl: iconEntities, isVisible: true },
    { id: "classification", title: "Classifications", isActive: isClassificationActive, iconUrl: iconClassifications, isVisible: true },
    { id: "glossary", title: "Glossary", isActive: isGlossaryActive, iconUrl: iconGlossary, isVisible: true },
    { id: "businessMetadata", title: "Business Metadata", isActive: isBusinessMetadataActive, iconUrl: iconBusinessMetadata, isVisible: true },
    { id: "relationships", title: "Relationships", isActive: isRelationshipActive, iconUrl: iconRelationships, isVisible: !!relationshipSearch },
    { id: "customFilters", title: "Custom Filters", isActive: isCustomFilterActive, iconUrl: iconCustomFilters, isVisible: true }
  ], [
    isEntitiesActive,
    isClassificationActive,
    isGlossaryActive,
    isBusinessMetadataActive,
    isRelationshipActive,
    isCustomFilterActive,
    relationshipSearch
  ]);

  const sidebarTreeModules = useMemo(
    () =>
      buildSidebarTreeModules(
        {
          entities: EntitiesTree,
          classification: ClassificationTree,
          glossary: GlossaryTree,
          businessMetadata: BusinessMetadataTree,
          relationships: RelationshipsTree,
          customFilters: CustomFiltersTree,
        },
        relationshipSearch
      ),
    [relationshipSearch]
  );

  const [popoverAnchor, setPopoverAnchor] = useState<HTMLButtonElement | null>(null);
  const [activePopover, setActivePopover] = useState<string | null>(null);
  const [popoverMaxHeight, setPopoverMaxHeight] = useState<string>("calc(100vh - 100px)");
  const [isBottomHalf, setIsBottomHalf] = useState<boolean>(false);
  const [popoverContainer, setPopoverContainer] = useState<HTMLDivElement | null>(null);
  const [sidebarRefs, setSidebarRefs] = useState<Record<string, HTMLDivElement | null>>({});
  const refCallbacks = useRef<Record<string, (el: HTMLDivElement | null) => void>>({});
  const setSidebarRef = useCallback((id: string) => {
    if (!refCallbacks.current[id]) {
      refCallbacks.current[id] = (el: HTMLDivElement | null) => {
        setSidebarRefs(prev => prev[id] === el ? prev : { ...prev, [id]: el });
      };
    }
    return refCallbacks.current[id];
  }, []);


  const handlePopoverOpen = (event: React.MouseEvent<HTMLButtonElement>, id: string) => {
    const target = event.currentTarget;

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

  const setPopoverPaperRef = useCallback(
    (el: HTMLDivElement | null) => {
      if (el) {
        el.style.setProperty("--popover-max-height", popoverMaxHeight);
      }
    },
    [popoverMaxHeight]
  );

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
                      <img src={iconSearch} className="sidebar-module-icon" alt="search" />
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
                  ref: setPopoverPaperRef,
                }}
              >
                {renderPopoverSearch()}
                <div className="sidebar-module-icon-container">
                  <Suspense fallback={<TreeSkeletonLoader count={2} />}>
                    <div
                      className="sidebar-treeview-container sidebar-toolbar"
                      ref={setPopoverContainer}
                    />
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
            className={`sidebar-wrapper ${!open ? "sidebar-wrapper--hidden" : ""}`}
          >
                {sidebarTreeModules
                  .filter((module) => module.visible)
                  .map((module) => (
                    <SidebarTreePortal
                      key={module.id}
                      moduleId={module.id}
                      dataCy={module.dataCy}
                      Tree={module.Tree}
                      searchTerm={searchTerm}
                      drawerOpen={open}
                      activePopover={activePopover}
                      popoverContainer={popoverContainer}
                      sidebarHost={sidebarRefs[module.id] ?? null}
                      setSidebarRef={setSidebarRef}
                    />
                  ))}
            </Paper>
          <div
            className={`sidebar-toggle-container ${open ? "sidebar-toggle-open" : "sidebar-toggle-closed"}`}
          >
            {open && (
              <div className="sidebar-version-container">
                <Typography variant="body2" className="sidebar-version-text">
                  {isVersionLoading ? (
                    <CircularProgress size={12} className="sidebar-version-loader" />
                  ) : versionData?.Version ? (
                    `V ${versionData.Version}`
                  ) : versionError ? (
                    'Version unavailable'
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
        className="sidebar-main-content"
      >
        {rightSideContent}
      </Main>
    </Stack>
  );
};

export default SideBarBody;
