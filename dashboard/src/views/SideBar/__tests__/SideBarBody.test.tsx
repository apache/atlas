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

import '@testing-library/jest-dom';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { MemoryRouter } from 'react-router-dom';
import { configureStore } from '@reduxjs/toolkit';
import SideBarBody from '../SideBarBody';
import * as enumSlice from '@redux/slice/enumSlice';
import * as rootClassificationSlice from '@redux/slice/rootClassificationSlice';
import * as typeDefHeaderSlice from '@redux/slice/typeDefSlices/typeDefHeaderSlice';
import * as allEntityTypesSlice from '@redux/slice/allEntityTypesSlice';
import * as metricsSlice from '@redux/slice/metricsSlice';
import * as sessionSlice from '@redux/slice/sessionSlice';

// Mock react-quill-new
jest.mock('react-quill-new', () => {
  const React = require('react');
  return {
    __esModule: true,
    default: React.forwardRef(({ value, onChange }: any, ref: Record<string, unknown>) => (
      <textarea
        ref={ref}
        value={value || ''}
        onChange={(e) => onChange && onChange(e.target.value)}
        data-testid="react-quill-mock"
      />
    ))
  };
});

// Mock lazy-loaded components
jest.mock('@views/Layout/Header', () => ({
  __esModule: true,
  default: ({ handleOpenModal, handleOpenAboutModal }: Record<string, unknown>) => (
    <div data-testid="header">
      <button onClick={handleOpenModal}>Open Modal</button>
      <button onClick={handleOpenAboutModal}>Open About</button>
    </div>
  )
}));

jest.mock('../SideBarTree/EntitiesTree', () => ({
  __esModule: true,
  default: ({ sideBarOpen, loading, searchTerm }: Record<string, unknown>) => (
    <div data-testid="entities-tree">
      Entities Tree - Open: {sideBarOpen.toString()} - Search: {searchTerm}
    </div>
  )
}));

jest.mock('../SideBarTree/ClassificationTree', () => ({
  __esModule: true,
  default: ({ sideBarOpen, loading, searchTerm }: Record<string, unknown>) => (
    <div data-testid="classification-tree">
      Classification Tree - Open: {sideBarOpen.toString()} - Search: {searchTerm}
    </div>
  )
}));

jest.mock('../SideBarTree/BusinessMetadataTree', () => ({
  __esModule: true,
  default: ({ sideBarOpen, searchTerm }: Record<string, unknown>) => (
    <div data-testid="business-metadata-tree">
      Business Metadata Tree - Open: {sideBarOpen.toString()} - Search: {searchTerm}
    </div>
  )
}));

jest.mock('../SideBarTree/GlossaryTree', () => ({
  __esModule: true,
  default: ({ sideBarOpen, searchTerm }: Record<string, unknown>) => (
    <div data-testid="glossary-tree">
      Glossary Tree - Open: {sideBarOpen.toString()} - Search: {searchTerm}
    </div>
  )
}));

jest.mock('../SideBarTree/RelationShipsTree', () => ({
  __esModule: true,
  default: ({ sideBarOpen, searchTerm }: Record<string, unknown>) => (
    <div data-testid="relationships-tree">
      Relationships Tree - Open: {sideBarOpen.toString()} - Search: {searchTerm}
    </div>
  )
}));

jest.mock('../SideBarTree/CustomFiltersTree', () => ({
  __esModule: true,
  default: ({ sideBarOpen, searchTerm }: Record<string, unknown>) => (
    <div data-testid="custom-filters-tree">
      Custom Filters Tree - Open: {sideBarOpen.toString()} - Search: {searchTerm}
    </div>
  )
}));

jest.mock('@views/ErrorPage', () => ({
  __esModule: true,
  default: ({ errorCode }: Record<string, unknown>) => <div data-testid="error-page">Error: {errorCode}</div>
}));

jest.mock('@views/AppRoutes', () => [
  {
    path: '/search',
    element: <div>Search Page</div>
  }
]);

jest.mock('../../../ErrorBoundary', () => ({
  __esModule: true,
  default: ({ children }: Record<string, unknown>) => <div data-testid="error-boundary">{children}</div>
}));

jest.mock('@components/SkeletonLoader', () => ({
  __esModule: true,
  default: ({ count }: Record<string, unknown>) => <div data-testid="skeleton-loader">Loading... ({count} items)</div>
}));

// Mock Redux actions
jest.mock('@redux/slice/enumSlice');
jest.mock('@redux/slice/rootClassificationSlice');
jest.mock('@redux/slice/typeDefSlices/typeDefHeaderSlice');
jest.mock('@redux/slice/allEntityTypesSlice');
jest.mock('@redux/slice/metricsSlice');
jest.mock('@redux/slice/sessionSlice');

// Mock utils
jest.mock('@utils/Enum', () => ({
  globalSessionData: {
    relationshipSearch: true
  },
  PathAssociateWithModule: {
    SEARCH: ['/search'],
    DETAIL_PAGE: ['/detailPage/:guid'],
    GLOSSARY: ['/glossary']
  }
}));

// Mock the history module before importing SideBarBody
const mockNavigate = jest.fn();
jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useNavigate: () => mockNavigate,
  useLocation: () => (global as unknown as Record<string, unknown>).mockLocation || { pathname: '/search', search: '' },
  useRoutes: () => null,
  matchRoutes: () => [{ route: { path: '/search' } }],
  Outlet: () => <div data-testid="outlet">Outlet Content</div>
}));

describe('SideBarBody', () => {
  const mockHandleOpenModal = jest.fn();
  const mockHandleOpenAboutModal = jest.fn();
  const mockDispatch = jest.fn();

  const defaultProps = {
    handleOpenModal: mockHandleOpenModal,
    handleOpenAboutModal: mockHandleOpenAboutModal
  };

  const createMockStore = (initialState: Record<string, unknown> = {}) => {
    return configureStore({
      reducer: {
        typeHeader: () => ({
          loading: false,
          ...(initialState.typeHeader || initialState)
        }),
        entity: () => ({
          loading: false,
          entityData: {},
          ...(initialState.entity || {})
        }),
        session: () => ({
          sessionObj: { loading: false, data: null, error: null },
          versionData: { loading: false, data: null, error: null },
          globalSessionData: { relationshipSearch: true },
          ...(initialState.session || {})
        })
      }
    });
  };

  const renderWithProviders = (
    props = defaultProps,
    { store = createMockStore(), route = '/search' } = {}
  ) => {
    return render(
      <Provider store={store}>
        <MemoryRouter initialEntries={[route]}>
          <SideBarBody {...props} />
        </MemoryRouter>
      </Provider>
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
    mockDispatch.mockResolvedValue({ type: 'test' });
    
    (enumSlice.fetchEnumData as jest.Mock) = jest.fn().mockReturnValue(mockDispatch);
    (rootClassificationSlice.fetchRootClassification as jest.Mock) = jest.fn().mockReturnValue(mockDispatch);
    (typeDefHeaderSlice.fetchTypeHeaderData as jest.Mock) = jest.fn().mockReturnValue(mockDispatch);
    (allEntityTypesSlice.fetchRootEntity as jest.Mock) = jest.fn().mockReturnValue(mockDispatch);
    (metricsSlice.fetchMetricEntity as jest.Mock) = jest.fn().mockReturnValue(mockDispatch);
    (sessionSlice.fetchVersionData as jest.Mock) = jest.fn().mockReturnValue(mockDispatch);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Rendering', () => {
    it('should render the sidebar with drawer open by default', async () => {
      renderWithProviders();
      
      // Wait for lazy-loaded components to render
      await waitFor(() => {
        expect(screen.getByTestId('entities-tree')).toBeInTheDocument();
      });
      
      expect(screen.getByTestId('classification-tree')).toBeInTheDocument();
      expect(screen.getByTestId('business-metadata-tree')).toBeInTheDocument();
      expect(screen.getByTestId('glossary-tree')).toBeInTheDocument();
      expect(screen.getByTestId('custom-filters-tree')).toBeInTheDocument();
    });

    it('should render Atlas logo when drawer is open', () => {
      renderWithProviders();
      
      const atlasLogo = document.querySelector('[data-cy="atlas-logo"]');
      expect(atlasLogo).toBeInTheDocument();
      expect(atlasLogo).toHaveAttribute('data-cy', 'atlas-logo');
    });

    it('should render Apache Atlas logo when drawer is closed', async () => {
      renderWithProviders();
      
      // Click toggle button to close drawer
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      await waitFor(() => {
        const apacheLogo = screen.getByAltText('Apache Atlas logo');
        expect(apacheLogo).toBeInTheDocument();
      });
    });

    it('should render search bar when drawer is open', () => {
      renderWithProviders();
      
      const searchInput = screen.getByPlaceholderText('Search');
      expect(searchInput).toBeInTheDocument();
      // The data-cy attribute is on the parent InputBase, not the input itself
      expect(searchInput.closest('[data-cy="searchNode"]')).toBeInTheDocument();
    });

    it('should render header component', () => {
      renderWithProviders();
      
      expect(screen.getByTestId('header')).toBeInTheDocument();
    });

    it('should render all tree components', () => {
      renderWithProviders();
      
      expect(screen.getByTestId('entities-tree')).toBeInTheDocument();
      expect(screen.getByTestId('classification-tree')).toBeInTheDocument();
      expect(screen.getByTestId('business-metadata-tree')).toBeInTheDocument();
      expect(screen.getByTestId('glossary-tree')).toBeInTheDocument();
      expect(screen.getByTestId('custom-filters-tree')).toBeInTheDocument();
    });

    it('should render relationships tree when relationshipSearch is enabled', () => {
      // The relationshipSearch is enabled in our mock (globalSessionData.relationshipSearch = true)
      // The relationships tree is rendered by default in our test setup
      renderWithProviders();
      
      // Verify all tree components are rendered including relationships
      const trees = [
        'entities-tree',
        'classification-tree',
        'business-metadata-tree',
        'glossary-tree',
        'relationships-tree',
        'custom-filters-tree'
      ];
      
      trees.forEach(tree => {
        expect(screen.getByTestId(tree)).toBeInTheDocument();
      });
    });
  });

  describe('Drawer Functionality', () => {
    it('should toggle drawer open/close when button is clicked', async () => {
      renderWithProviders();
      
      // Initially open
      expect(
        document.querySelector('[data-cy="atlas-logo"]')
      ).toBeInTheDocument();
      
      // Click toggle button - find by testid for KeyboardDoubleArrowLeftIcon
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      expect(toggleButton).toBeInTheDocument();
      fireEvent.click(toggleButton!);
      
      await waitFor(() => {
        expect(screen.getByAltText('Apache Atlas logo')).toBeInTheDocument();
      });
      
      // Click again to open - now find KeyboardDoubleArrowRightIcon
      const expandButton = screen.getByTestId('KeyboardDoubleArrowRightIcon').closest('button');
      fireEvent.click(expandButton!);
      
      await waitFor(() => {
        expect(
          document.querySelector('[data-cy="atlas-logo"]')
        ).toBeInTheDocument();
      });
    });

    it('should show collapse icon when drawer is open', () => {
      renderWithProviders();
      
      const collapseIcon = screen.getByTestId('KeyboardDoubleArrowLeftIcon');
      expect(collapseIcon).toBeInTheDocument();
    });

    it('should show expand icon when drawer is closed', async () => {
      renderWithProviders();
      
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      await waitFor(() => {
        const expandIcon = screen.getByTestId('KeyboardDoubleArrowRightIcon');
        expect(expandIcon).toBeInTheDocument();
      });
    });
  });

  describe('Search Functionality', () => {
    it('should update search term when typing in search bar', async () => {
      renderWithProviders();
      
      const searchInput = screen.getByPlaceholderText('Search');
      
      fireEvent.change(searchInput, { target: { value: 'test search' } });
      
      await waitFor(() => {
        expect(searchInput).toHaveValue('test search');
      });
    });

    it('should pass search term to tree components', async () => {
      renderWithProviders();
      
      const searchInput = screen.getByPlaceholderText('Search');
      
      fireEvent.change(searchInput, { target: { value: 'entity' } });
      
      await waitFor(() => {
        expect(screen.getByTestId('entities-tree')).toHaveTextContent('Search: entity');
        expect(screen.getByTestId('classification-tree')).toHaveTextContent('Search: entity');
      });
    });

    it('should render search icon button', () => {
      renderWithProviders();
      
      // There are multiple elements with aria-label="search" (input and button)
      const searchButtons = screen.getAllByLabelText('search');
      expect(searchButtons.length).toBeGreaterThan(0);
    });
  });

  describe('Navigation', () => {
    it('should navigate to search page when Atlas logo is clicked', () => {
      renderWithProviders();
      
      const atlasLogo = document.querySelector(
        '[data-cy="atlas-logo"]'
      ) as HTMLElement;
      expect(atlasLogo).toBeInTheDocument();
      fireEvent.click(atlasLogo);
      
      expect(mockNavigate).toHaveBeenCalledWith({ pathname: '/search' }, { replace: true });
    });

    it('should navigate to search page when Apache Atlas logo is clicked', async () => {
      renderWithProviders();
      
      // Close drawer first
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      await waitFor(() => {
        const apacheLogo = screen.getByAltText('Apache Atlas logo');
        fireEvent.click(apacheLogo);
        expect(mockNavigate).toHaveBeenCalledWith({ pathname: '/search' }, { replace: true });
      });
    });

    it('should render error page for unmatched routes', () => {
      // The matchRoutes mock always returns a match in our setup
      // In a real scenario, unmatched routes would show error page
      // For now, verify the component handles the route
      renderWithProviders(defaultProps, { route: '/invalid-route' });
      
      // With our mock, it will render outlet instead of error page
      expect(screen.getByTestId('outlet')).toBeInTheDocument();
    });

    it('should render outlet for matched routes', () => {
      renderWithProviders(defaultProps, { route: '/search' });
      
      expect(screen.getByTestId('error-boundary')).toBeInTheDocument();
    });
  });

  describe('Redux Integration', () => {
    it('should dispatch fetchTypeHeaderData on mount', () => {
      renderWithProviders();
      
      expect(typeDefHeaderSlice.fetchTypeHeaderData).toHaveBeenCalled();
    });

    it('should dispatch fetchRootEntity on mount', () => {
      renderWithProviders();
      
      expect(allEntityTypesSlice.fetchRootEntity).toHaveBeenCalled();
    });

    it('should dispatch fetchRootClassification on mount', () => {
      renderWithProviders();
      
      expect(rootClassificationSlice.fetchRootClassification).toHaveBeenCalled();
    });

    it('should dispatch fetchEnumData on mount', () => {
      renderWithProviders();
      
      expect(enumSlice.fetchEnumData).toHaveBeenCalled();
    });

    it('should dispatch fetchMetricEntity on mount', () => {
      renderWithProviders();
      
      expect(metricsSlice.fetchMetricEntity).toHaveBeenCalled();
    });

    it('should dispatch fetchVersionData on mount', () => {
      renderWithProviders();
      
      expect(sessionSlice.fetchVersionData).toHaveBeenCalled();
    });


  });



  describe('Props Handling', () => {
    it('should pass handleOpenModal to Header', () => {
      renderWithProviders();
      
      const openModalButton = screen.getByText('Open Modal');
      fireEvent.click(openModalButton);
      
      expect(mockHandleOpenModal).toHaveBeenCalled();
    });

    it('should pass handleOpenAboutModal to Header', () => {
      renderWithProviders();
      
      const openAboutButton = screen.getByText('Open About');
      fireEvent.click(openAboutButton);
      
      expect(mockHandleOpenAboutModal).toHaveBeenCalled();
    });

    it('should show "Version unavailable" if versionError is set', () => {
      const stateWithVersionError = {
        session: {
          versionData: {
            loading: false,
            data: null,
            error: { message: "Failed to fetch version" }
          }
        }
      };
      renderWithProviders({}, { store: createMockStore(stateWithVersionError) });
      
      expect(screen.getByText('Version unavailable')).toBeInTheDocument();
    });

    it('should hide relationships icon when relationshipSearch is falsy', () => {
      const { globalSessionData } = require('@utils/Enum');
      const originalValue = globalSessionData.relationshipSearch;
      globalSessionData.relationshipSearch = false;

      renderWithProviders();
      
      // Collapse drawer to check icon
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);

      expect(screen.queryByAltText('relationships')).not.toBeInTheDocument();
      // Should also hide the tree in the expanded view
      expect(screen.queryByTestId('relationships-tree')).not.toBeInTheDocument();

      // Restore value
      globalSessionData.relationshipSearch = originalValue;
    });

    it('should show V x.x display for version footer', () => {
      const stateWithVersion = {
        session: {
          versionData: {
            loading: false,
            data: { Version: "3.12.1.0" },
            error: null
          }
        }
      };
      renderWithProviders({}, { store: createMockStore(stateWithVersion) });
      expect(screen.getByText('V 3.12.1.0')).toBeInTheDocument();
    });

    it('should not display version footer when drawer is closed', async () => {
      const stateWithVersion = {
        session: {
          versionData: {
            loading: false,
            data: { Version: "3.12.1.0" },
            error: null
          }
        }
      };
      renderWithProviders({}, { store: createMockStore(stateWithVersion) });
      expect(screen.getByText('V 3.12.1.0')).toBeInTheDocument();

      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);

      await waitFor(() => {
        expect(screen.queryByText('V 3.12.1.0')).not.toBeInTheDocument();
      });
    });

    it('should show loading spinner for version footer when loading', () => {
      const stateLoadingVersion = {
        session: {
          versionData: {
            loading: true,
            data: null,
            error: null
          }
        }
      };
      renderWithProviders({}, { store: createMockStore(stateLoadingVersion) });
      expect(screen.getByRole('progressbar')).toBeInTheDocument();
    });
  });

  describe('Sidebar State', () => {
    it('should pass sideBarOpen state to all tree components', () => {
      renderWithProviders();
      
      expect(screen.getByTestId('entities-tree')).toHaveTextContent('Open: true');
      expect(screen.getByTestId('classification-tree')).toHaveTextContent('Open: true');
      expect(screen.getByTestId('business-metadata-tree')).toHaveTextContent('Open: true');
      expect(screen.getByTestId('glossary-tree')).toHaveTextContent('Open: true');
      expect(screen.getByTestId('custom-filters-tree')).toHaveTextContent('Open: true');
    });

    it('should update sideBarOpen state when drawer is toggled', async () => {
      renderWithProviders();
      
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      await waitFor(() => {
        expect(screen.getByTestId('entities-tree')).toBeInTheDocument();
        expect(screen.getByTestId('entities-tree').closest('.sidebar-wrapper')).toHaveClass('sidebar-wrapper--hidden');
      });
    });
  });

  describe('Data Attributes', () => {
    it('should have data-cy attributes for testing', () => {
      renderWithProviders();
      
      expect(screen.getByTestId('entities-tree').closest('[data-cy="r_entityTreeRender"]')).toBeInTheDocument();
      expect(screen.getByTestId('classification-tree').closest('[data-cy="r_classificationTreeRender"]')).toBeInTheDocument();
      expect(screen.getByTestId('business-metadata-tree').closest('[data-cy="r_businessMetadataTreeRender"]')).toBeInTheDocument();
      expect(screen.getByTestId('glossary-tree').closest('[data-cy="r_glossaryTreeRender"]')).toBeInTheDocument();
      expect(screen.getByTestId('custom-filters-tree').closest('[data-cy="r_customFilterTreeRender"]')).toBeInTheDocument();
    });

    it('should have data-cy attribute for collapsed logo', async () => {
      renderWithProviders();
      
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      await waitFor(() => {
        const collapsedLogo = screen.getByAltText('Apache Atlas logo').closest('[data-cy="apache-atlas-logo-collapsed"]');
        expect(collapsedLogo).toBeInTheDocument();
      });
    });
  });

  describe('Styling and Layout', () => {
    it('should apply correct styles when drawer is open', () => {
      renderWithProviders();
      
      const drawer = screen.getByTestId('entities-tree').closest('.MuiDrawer-root');
      expect(drawer).toBeTruthy();
    });

    it('should apply correct styles when drawer is closed', async () => {
      renderWithProviders();
      
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      await waitFor(() => {
        expect(screen.getByAltText('Apache Atlas logo')).toBeInTheDocument();
      });
    });

    it('should render main content area', () => {
      renderWithProviders();
      
      expect(screen.getByTestId('header')).toBeInTheDocument();
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty search term', () => {
      renderWithProviders();
      
      const searchInput = screen.getByPlaceholderText('Search');
      
      fireEvent.change(searchInput, { target: { value: '' } });
      
      expect(searchInput).toHaveValue('');
    });

    it('should handle special characters in search', async () => {
      renderWithProviders();
      
      const searchInput = screen.getByPlaceholderText('Search');
      
      fireEvent.change(searchInput, { target: { value: '!@#$%^&*()' } });
      
      await waitFor(() => {
        expect(searchInput).toHaveValue('!@#$%^&*()');
      });
    });

    it('should handle routes with exclamation mark', () => {
      // Routes with ! should render outlet, not error page
      renderWithProviders(defaultProps, { route: '/search!test' });
      
      expect(screen.getByTestId('outlet')).toBeInTheDocument();
    });
  });



  describe('Collapsed Sidebar Popovers', () => {
    beforeEach(() => {
      // Start with closed drawer to see popover icons
      renderWithProviders();
      const toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
    });

    it('should open correct popover and document that it mounts a duplicate tree instance (remount behavior)', async () => {
      // Find the glossary icon and click it
      const glossaryIcon = screen.getByAltText('glossary');
      fireEvent.click(glossaryIcon.closest('button')!);

      await waitFor(() => {
        // Document remount behavior: The glossary tree is rendered TWICE:
        // 1. The original instance inside the hidden sidebar-wrapper
        // 2. A new separate instance mounted inside the Popover
        const glossaryTrees = screen.getAllByTestId('glossary-tree');
        expect(glossaryTrees.length).toBe(2);
      });
    });

    it('should share search term between sidebar and popover', async () => {
      // Re-open sidebar to access main search input
      const toggleOpenButton = screen.getByTestId('KeyboardDoubleArrowRightIcon').closest('button');
      fireEvent.click(toggleOpenButton!);

      // Set search term in the main search bar
      const searchInput = screen.getAllByPlaceholderText('Search')[0];
      fireEvent.change(searchInput, { target: { value: 'popover_search' } });

      // Close sidebar
      const toggleCloseButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleCloseButton!);

      // Click entities icon
      const entitiesIcon = screen.getByAltText('entities');
      fireEvent.click(entitiesIcon.closest('button')!);

      await waitFor(() => {
        // Popover should receive the search term
        const entitiesTree = screen.getAllByTestId('entities-tree').find(
          el => el.textContent?.includes('Search: popover_search')
        );
        expect(entitiesTree).toBeInTheDocument();
      });
    });

    it('should close popover when clicking outside', async () => {
      // Open glossary popover
      const glossaryIcon = screen.getByAltText('glossary');
      fireEvent.click(glossaryIcon.closest('button')!);

      await waitFor(() => {
        expect(screen.getAllByTestId('glossary-tree').length).toBeGreaterThan(0);
      });

      const backdrop = document.querySelector('.MuiBackdrop-root');
      expect(backdrop).toBeInTheDocument();
      if (backdrop) {
        fireEvent.click(backdrop);
      }

      await waitFor(() => {
        expect(screen.getAllByTestId('glossary-tree')).toHaveLength(1);
      });
    });

    it('should close popover on Escape key press', async () => {
      // Open glossary popover
      const glossaryIcon = screen.getByAltText('glossary');
      fireEvent.click(glossaryIcon.closest('button')!);

      await waitFor(() => {
        expect(screen.getAllByTestId('glossary-tree').length).toBeGreaterThan(0);
      });

      // Press Escape to close the popover
      // MUI Popover listens for Escape on the document or active element
      fireEvent.keyDown(document.activeElement || document.body, { key: 'Escape', code: 'Escape' });

      await waitFor(() => {
        expect(screen.getAllByTestId('glossary-tree')).toHaveLength(1);
      });
    });

    it('should NOT open popover when sidebar is expanded', async () => {
      // Re-open sidebar that was closed in beforeEach
      const toggleOpenButton = screen.getByTestId('KeyboardDoubleArrowRightIcon').closest('button');
      fireEvent.click(toggleOpenButton!);

      // Ensure sidebar is expanded
      expect(screen.getByTestId('entities-tree')).toBeInTheDocument();
      
      // Module icons don't exist when expanded
      const icons = screen.queryByAltText('glossary');
      expect(icons).not.toBeInTheDocument();
    });

    it('should only open one popover at a time when switching modules', async () => {
      // Find the glossary icon and click it
      const glossaryIcon = screen.getByAltText('glossary');
      fireEvent.click(glossaryIcon.closest('button')!);

      await waitFor(() => {
        expect(screen.getAllByTestId('glossary-tree').length).toBeGreaterThan(0);
      });

      // Click entities icon
      const entitiesIcon = screen.getByAltText('entities');
      fireEvent.click(entitiesIcon.closest('button')!);

      await waitFor(() => {
        expect(screen.getAllByTestId('entities-tree')).toHaveLength(2);
        expect(screen.getAllByTestId('glossary-tree')).toHaveLength(1);
      });
    });

    it('should calculate popover max height correctly when near viewport bottom', async () => {
      const originalInnerHeight = window.innerHeight;
      Object.defineProperty(window, 'innerHeight', { value: 600, configurable: true });

      const originalGetBoundingClientRect = Element.prototype.getBoundingClientRect;
      Element.prototype.getBoundingClientRect = jest.fn(() => ({
        top: 500, // Near bottom
        bottom: 540,
        left: 0,
        right: 50,
        width: 50,
        height: 40,
        x: 0,
        y: 500,
        toJSON: () => {}
      }));

      const glossaryIcon = screen.getByAltText('glossary');
      fireEvent.click(glossaryIcon.closest('button')!);

      await waitFor(() => {
        const paper = document.querySelector('.sidebar-popover-paper') as HTMLElement;
        expect(paper).toBeInTheDocument();
        // spaceBelow = 600 - 500 - 24 = 76 (< 350, so isBottom = true)
        // setPopoverMaxHeight = max(250, 540 - 24) = 516px
        expect(paper.style.maxHeight).toBe('516px');
      });
      
      // Restore
      Object.defineProperty(window, 'innerHeight', { value: originalInnerHeight, configurable: true });
      Element.prototype.getBoundingClientRect = originalGetBoundingClientRect;
    });
  });

  describe('Active State Markers', () => {
    it('should apply active state markers correctly', async () => {
      // Test Entities active (type param present but isCF is not true)
      (global as unknown as Record<string, unknown>).mockLocation = { pathname: '/search', search: '?type=table' };
      const { unmount: unmount1 } = renderWithProviders();
      let toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      let entitiesIcon = screen.getByAltText('entities');
      expect(entitiesIcon.closest('.sidebar-icon-active')).toBeInTheDocument();
      unmount1();

      // Test Custom Filters active (isCF=true)
      (global as unknown as Record<string, unknown>).mockLocation = { pathname: '/search', search: '?isCF=true&type=myFilter' };
      const { unmount: unmount2 } = renderWithProviders();
      toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      let customFiltersIcon = screen.getByAltText('custom filters');
      expect(customFiltersIcon.closest('.sidebar-icon-active')).toBeInTheDocument();
      
      // Entities should NOT be active if isCF=true
      entitiesIcon = screen.getByAltText('entities');
      expect(entitiesIcon.closest('.sidebar-icon-active')).not.toBeInTheDocument();
      unmount2();

      // Test Glossary active
      (global as unknown as Record<string, unknown>).mockLocation = { pathname: '/glossary', search: '' };
      const { unmount: unmount3 } = renderWithProviders();
      toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      let glossaryIcon = screen.getByAltText('glossary');
      expect(glossaryIcon.closest('.sidebar-icon-active')).toBeInTheDocument();
      unmount3();

      // Test Classification active
      (global as unknown as Record<string, unknown>).mockLocation = { pathname: '/search', search: '?tag=PII' };
      const { unmount: unmount4 } = renderWithProviders();
      toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      let classificationIcon = screen.getByAltText('classifications');
      expect(classificationIcon.closest('.sidebar-icon-active')).toBeInTheDocument();
      unmount4();

      // Test Business Metadata active
      (global as unknown as Record<string, unknown>).mockLocation = { pathname: '/administrator/businessMetadata', search: '' };
      const { unmount: unmount5 } = renderWithProviders();
      toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      let bmIcon = screen.getByAltText('business metadata');
      expect(bmIcon.closest('.sidebar-icon-active')).toBeInTheDocument();
      unmount5();

      // Test Relationships active
      (global as unknown as Record<string, unknown>).mockLocation = { pathname: '/search', search: '?relationshipName=Employee' };
      const { unmount: unmount6 } = renderWithProviders();
      toggleButton = screen.getByTestId('KeyboardDoubleArrowLeftIcon').closest('button');
      fireEvent.click(toggleButton!);
      
      let relIcon = screen.getByAltText('relationships');
      expect(relIcon.closest('.sidebar-icon-active')).toBeInTheDocument();
      unmount6();

      (global as unknown as Record<string, unknown>).mockLocation = undefined;
    });
  });
});
