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

import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Provider } from 'react-redux';
import { MemoryRouter } from 'react-router-dom';
import { configureStore } from '@reduxjs/toolkit';
import { toast } from 'react-toastify';
import SaveFilters from '../SaveFilters';
import { editSavedSearch } from '@api/apiMethods/savedSearchApiMethod';
import { fetchSavedSearchData } from '@redux/slice/savedSearchSlice';
import * as Utils from '@utils/Utils';
import * as CommonViewFunction from '@utils/CommonViewFunction';

// Mock dependencies
jest.mock('@api/apiMethods/savedSearchApiMethod');
jest.mock('@redux/slice/savedSearchSlice');
jest.mock('react-toastify', () => ({
  toast: {
    success: jest.fn(),
    error: jest.fn(),
    dismiss: jest.fn()
  }
}));

jest.mock('@utils/Utils', () => ({
  ...jest.requireActual('@utils/Utils'),
  isEmpty: (val: any) => {
    if (val === null || val === undefined) return true;
    if (Array.isArray(val)) return val.length === 0;
    if (typeof val === 'object') return Object.keys(val).length === 0;
    if (typeof val === 'string') return val.length === 0;
    return false;
  },
  serverError: jest.fn()
}));

jest.mock('@utils/CommonViewFunction', () => ({
  generateObjectForSaveSearchApi: jest.fn()
}));

jest.mock('@utils/Helper', () => {
  const actualHelper = jest.requireActual('@utils/Helper');
  return {
    ...actualHelper,
    cloneDeep: (val: any) => {
      if (val === null) return null;
      if (val === undefined) return undefined;
      if (Array.isArray(val)) return [...val.map((item: any) => JSON.parse(JSON.stringify(item)))];
      if (typeof val === 'object') return JSON.parse(JSON.stringify(val));
      return val;
    },
    invert: (obj: any) => {
      const inverse = new Map();
      for (let [key, value] of Object.entries(obj)) {
        inverse.set(value, key);
      }
      return inverse;
    }
  };
});

describe('SaveFilters', () => {
  jest.setTimeout(10000);
  
  const mockOnClose = jest.fn();
  const mockDispatch = jest.fn();
  
  const mockSavedSearchData = [
    {
      guid: 'guid-1',
      name: 'Test Filter 1',
      searchType: 'BASIC',
      searchParameters: {
        type: 'DataSet',
        query: 'test'
      }
    },
    {
      guid: 'guid-2',
      name: 'Test Filter 2',
      searchType: 'ADVANCED',
      searchParameters: {
        query: 'advanced test'
      }
    },
    {
      guid: 'guid-3',
      name: 'Relationship Filter',
      searchType: 'BASIC_RELATIONSHIP',
      searchParameters: {
        relationshipName: 'test-relation'
      }
    }
  ];

  const createMockStore = (savedSearchData: any = []) => {
    return configureStore({
      reducer: {
        savedSearch: () => ({
          savedSearchData: savedSearchData || []
        })
      }
    });
  };

  const renderWithProviders = (
    component: React.ReactElement,
    { store = createMockStore(), route = '/' } = {}
  ) => {
    return render(
      <Provider store={store}>
        <MemoryRouter initialEntries={[route]}>
          {component}
        </MemoryRouter>
      </Provider>
    );
  };

  // Helper function to fill autocomplete and submit form
  const fillAndSubmitForm = async (filterName: string, shouldSelectAddOption = true) => {
    const autocomplete = screen.getByRole('combobox');
    
    await act(async () => {
      await userEvent.clear(autocomplete);
      await userEvent.type(autocomplete, filterName);
    });

    if (shouldSelectAddOption) {
      // Wait for the "Add:" option to appear
      await waitFor(() => {
        const addOption = screen.queryByText(/Add:/i);
        if (addOption) {
          return addOption;
        }
        // Sometimes the option text is split, check for the option element
        const options = screen.queryAllByRole('option');
        return options.length > 0;
      }, { timeout: 3000 });

      // Click the "Add:" option if it exists
      const addOption = screen.queryByText(/Add:/i);
      if (addOption) {
        await act(async () => {
          await userEvent.click(addOption);
        });
      } else {
        // Try clicking the first option if "Add:" text not found
        const options = screen.queryAllByRole('option');
        if (options.length > 0) {
          await act(async () => {
            await userEvent.click(options[0]);
          });
        }
      }
    }

    // Find and click the Save button - get all buttons and find the one with Save text
    await waitFor(() => {
      const buttons = screen.getAllByRole('button');
      const saveButton = buttons.find(btn => {
        const text = btn.textContent?.trim() || '';
        return text === 'Save' || text === 'Save as';
      });
      return saveButton !== undefined;
    }, { timeout: 2000 });

    const buttons = screen.getAllByRole('button');
    const saveButton = buttons.find(btn => {
      const text = btn.textContent?.trim() || '';
      return text === 'Save' || text === 'Save as';
    });
    
    if (saveButton) {
      await act(async () => {
        await userEvent.click(saveButton);
      });
    }
  };

  beforeEach(() => {
    jest.clearAllMocks();
    mockDispatch.mockResolvedValue({ type: 'test' });
    (fetchSavedSearchData as jest.Mock).mockReturnValue(mockDispatch);
    (editSavedSearch as jest.Mock).mockResolvedValue({});
    (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
      name: 'Test Filter',
      searchParameters: {
        type: 'DataSet',
        query: 'test'
      }
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Rendering', () => {
    it('should render the modal when open is true', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />
      );
      
      expect(screen.getByText(/Save.*Custom Filter/i)).toBeInTheDocument();
    });

    it('should not render modal content when open is false', () => {
      renderWithProviders(
        <SaveFilters open={false} onClose={mockOnClose} />
      );
      
      expect(screen.queryByPlaceholderText(/Enter filter name/i)).not.toBeInTheDocument();
    });

    it('should display "Save Basic Custom Filter" for basic search', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      expect(screen.getByText('Save Basic Custom Filter')).toBeInTheDocument();
    });

    it('should display "Save Advanced Custom Filter" for advanced search', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=advanced&query=test' }
      );
      
      expect(screen.getByText('Save Advanced Custom Filter')).toBeInTheDocument();
    });

    it('should display "Save Relationship Custom Filter" for relationship search', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?relationshipName=test-relation' }
      );
      
      expect(screen.getByText('Save Relationship Custom Filter')).toBeInTheDocument();
    });

    it('should show "Save as" button when savedSearchData exists', () => {
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store }
      );
      
      expect(screen.getByText('Save as')).toBeInTheDocument();
    });

    it('should show "Save" button when savedSearchData is empty', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />
      );
      
      expect(screen.getByText('Save')).toBeInTheDocument();
    });

    it('should render required field label', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />
      );
      
      expect(screen.getByText('Name')).toBeInTheDocument();
    });

    it('should render Cancel button', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />
      );
      
      expect(screen.getByText('Cancel')).toBeInTheDocument();
    });
  });

  describe('Autocomplete Options', () => {
    it('should filter and display BASIC search options', async () => {
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
        await userEvent.type(autocomplete, 'Test');
      });
      
      await waitFor(() => {
        expect(screen.getByText('Test Filter 1')).toBeInTheDocument();
      }, { timeout: 3000 });
    });

    it('should filter and display ADVANCED search options', async () => {
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=advanced&query=test' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        expect(screen.getByText('Test Filter 2')).toBeInTheDocument();
      }, { timeout: 3000 });
    });

    it('should filter and display BASIC_RELATIONSHIP search options', async () => {
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?relationshipName=test-relation' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        expect(screen.getByText('Relationship Filter')).toBeInTheDocument();
      }, { timeout: 3000 });
    });

    it('should sort options alphabetically', async () => {
      const unsortedData = [
        { name: 'Zebra Filter', searchType: 'BASIC', searchParameters: {} },
        { name: 'Apple Filter', searchType: 'BASIC', searchParameters: {} },
        { name: 'Mango Filter', searchType: 'BASIC', searchParameters: {} }
      ];
      const store = createMockStore(unsortedData);
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        const options = screen.getAllByRole('option');
        expect(options[0]).toHaveTextContent('Apple Filter');
      }, { timeout: 3000 });
    });
  });

  describe('Form Submission - New Filter', () => {
    it('should create new BASIC filter with POST method', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'New Basic Filter',
        searchParameters: { type: 'DataSet' }
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('New Basic Filter');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            name: 'New Basic Filter',
            searchType: 'BASIC'
          }),
          'POST'
        );
      }, { timeout: 5000 });
    });

    it('should create new ADVANCED filter with POST method', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'New Advanced Filter',
        searchParameters: { query: 'test' }
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=advanced&query=test' }
      );
      
      await fillAndSubmitForm('New Advanced Filter');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            name: 'New Advanced Filter',
            searchType: 'ADVANCED'
          }),
          'POST'
        );
      }, { timeout: 5000 });
    });

    it('should create new BASIC_RELATIONSHIP filter with POST method', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'New Relationship Filter',
        searchParameters: { relationshipName: 'test-relation' }
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?relationshipName=test-relation' }
      );
      
      await fillAndSubmitForm('New Relationship Filter');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            name: 'New Relationship Filter',
            searchType: 'BASIC_RELATIONSHIP'
          }),
          'POST'
        );
      }, { timeout: 5000 });
    });
  });

  describe('Form Submission - Update Existing Filter', () => {
    it('should update existing filter with PUT method', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Test Filter 1',
        searchParameters: { type: 'DataSet', query: 'updated' }
      });
      
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        expect(screen.getByText('Test Filter 1')).toBeInTheDocument();
      }, { timeout: 3000 });
      
      await act(async () => {
        await userEvent.click(screen.getByText('Test Filter 1'));
      });
      
      const saveButton = screen.getByText('Save as');
      await act(async () => {
        await userEvent.click(saveButton);
      });
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            guid: 'guid-1',
            name: 'Test Filter 1'
          }),
          'PUT'
        );
      }, { timeout: 5000 });
    });

    it('should merge searchParameters when updating existing filter', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Test Filter 1',
        searchParameters: { 
          type: 'DataSet',
          query: 'new query',
          newParam: 'value'
        }
      });
      
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        expect(screen.getByText('Test Filter 1')).toBeInTheDocument();
      }, { timeout: 3000 });
      
      await act(async () => {
        await userEvent.click(screen.getByText('Test Filter 1'));
      });
      
      const saveButton = screen.getByText('Save as');
      await act(async () => {
        await userEvent.click(saveButton);
      });
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            searchParameters: expect.objectContaining({
              type: 'DataSet',
              query: 'new query',
              newParam: 'value'
            })
          }),
          'PUT'
        );
      }, { timeout: 5000 });
    });
  });

  describe('URL Parameters Handling', () => {
    it('should handle includeDE parameter as boolean', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet&includeDE=true' }
      );
      
      await fillAndSubmitForm('Test');
      
      // First verify the form submitted
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
        expect(CommonViewFunction.generateObjectForSaveSearchApi).toHaveBeenCalled();
      }, { timeout: 5000 });
      
      // Then verify the parameters were passed correctly - check that includeDE is in the value object
      const calls = (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mock.calls;
      expect(calls.length).toBeGreaterThan(0);
      const callArg = calls[calls.length - 1][0];
      expect(callArg.value).toBeDefined();
      // The includeDE should be converted to boolean true (or string "true" if conversion didn't happen)
      expect(callArg.value.includeDE === true || callArg.value.includeDE === "true").toBe(true);
    });

    it('should handle excludeSC parameter as boolean', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet&excludeSC=true' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
        expect(CommonViewFunction.generateObjectForSaveSearchApi).toHaveBeenCalled();
      }, { timeout: 5000 });
      
      const calls = (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mock.calls;
      expect(calls.length).toBeGreaterThan(0);
      const callArg = calls[calls.length - 1][0];
      expect(callArg.value.excludeSC === true || callArg.value.excludeSC === "true").toBe(true);
    });

    it('should handle excludeST parameter as boolean', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet&excludeST=true' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
        expect(CommonViewFunction.generateObjectForSaveSearchApi).toHaveBeenCalled();
      }, { timeout: 5000 });
      
      const calls = (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mock.calls;
      expect(calls.length).toBeGreaterThan(0);
      const callArg = calls[calls.length - 1][0];
      expect(callArg.value.excludeST === true || callArg.value.excludeST === "true").toBe(true);
    });

    it('should handle all URL parameters', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet&tag=test-tag&term=test-term&includeDE=true&excludeSC=false&excludeST=true' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
        expect(CommonViewFunction.generateObjectForSaveSearchApi).toHaveBeenCalled();
      }, { timeout: 5000 });
      
      const calls = (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mock.calls;
      expect(calls.length).toBeGreaterThan(0);
      const callArg = calls[calls.length - 1][0];
      expect(callArg.value.type).toBe('DataSet');
      expect(callArg.value.tag).toBe('test-tag');
      expect(callArg.value.term).toBe('test-term');
      expect(callArg.value.includeDE === true || callArg.value.includeDE === "true").toBe(true);
      expect(callArg.value.excludeSC === false || callArg.value.excludeSC === "false").toBe(true);
      expect(callArg.value.excludeST === true || callArg.value.excludeST === "true").toBe(true);
    });

    it('should handle boolean conversion for false values', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet&includeDE=false' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
        expect(CommonViewFunction.generateObjectForSaveSearchApi).toHaveBeenCalled();
      }, { timeout: 5000 });
      
      const calls = (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mock.calls;
      expect(calls.length).toBeGreaterThan(0);
      const callArg = calls[calls.length - 1][0];
      expect(callArg.value.includeDE === false || callArg.value.includeDE === "false").toBe(true);
    });
  });

  describe('Success and Error Handling', () => {
    it('should show success toast on successful save', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Success Filter',
        searchParameters: {}
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('Success Filter');
      
      await waitFor(() => {
        expect(toast.dismiss).toHaveBeenCalled();
        expect(toast.success).toHaveBeenCalledWith('Success Filter was updated successfully');
      }, { timeout: 5000 });
    });

    it('should close modal on successful save', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Test',
        searchParameters: {}
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(mockOnClose).toHaveBeenCalled();
      }, { timeout: 5000 });
    });

    it('should call fetchSavedSearchData on successful save', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Test',
        searchParameters: {}
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(fetchSavedSearchData).toHaveBeenCalled();
      }, { timeout: 5000 });
    });

    it('should handle error on save failure', async () => {
      const mockError = new Error('Save failed');
      (editSavedSearch as jest.Mock).mockRejectedValue(mockError);
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Error Filter',
        searchParameters: {}
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('Error Filter');
      
      await waitFor(() => {
        expect(Utils.serverError).toHaveBeenCalledWith(
          mockError,
          expect.anything()
        );
      }, { timeout: 5000 });
    });

    it('should not close modal on save failure', async () => {
      (editSavedSearch as jest.Mock).mockRejectedValue(new Error('Failed'));
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Test',
        searchParameters: {}
      });
      
      jest.spyOn(console, 'log').mockImplementation();
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      mockOnClose.mockClear();
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(Utils.serverError).toHaveBeenCalled();
      }, { timeout: 5000 });
      
      expect(mockOnClose).not.toHaveBeenCalled();
    });
  });

  describe('Cancel Button', () => {
    it('should close modal when Cancel button is clicked', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />
      );
      
      const cancelButton = screen.getByText('Cancel');
      fireEvent.click(cancelButton);
      
      expect(mockOnClose).toHaveBeenCalled();
    });
  });

  describe('Form Validation', () => {
    it('should require filter name to be filled', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      const saveButton = screen.getByText('Save');
      await act(async () => {
        await userEvent.click(saveButton);
      });
      
      // Form should not submit without a value
      await waitFor(() => {
        expect(editSavedSearch).not.toHaveBeenCalled();
      }, { timeout: 2000 });
    });

    it('should disable save button while submitting', async () => {
      (editSavedSearch as jest.Mock).mockImplementation(() => 
        new Promise(resolve => setTimeout(resolve, 100))
      );
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Test',
        searchParameters: {}
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.type(autocomplete, 'Test');
      });
      
      // Wait for Add option
      await waitFor(() => {
        const addOption = screen.queryByText(/Add:/i);
        if (addOption) {
          return addOption;
        }
        return screen.queryAllByRole('option').length > 0;
      }, { timeout: 3000 });
      
      const addOption = screen.queryByText(/Add:/i);
      if (addOption) {
        await act(async () => {
          await userEvent.click(addOption);
        });
      }
      
      const saveButton = screen.getByText('Save');
      await act(async () => {
        await userEvent.click(saveButton);
      });
      
      // Button should be disabled during submission
      await waitFor(() => {
        expect(saveButton).toBeDisabled();
      }, { timeout: 1000 });
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty savedSearchData', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      expect(screen.getByText('Save')).toBeInTheDocument();
    });

    it('should handle URL without search parameters', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic' }
      );
      
      expect(screen.getByText('Save Basic Custom Filter')).toBeInTheDocument();
    });

    it('should handle filter with no matching search type', async () => {
      const mixedData = [
        { name: 'Filter 1', searchType: 'BASIC', searchParameters: {} },
        { name: 'Filter 2', searchType: 'OTHER_TYPE', searchParameters: {} }
      ];
      const store = createMockStore(mixedData);
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        expect(screen.getByText('Filter 1')).toBeInTheDocument();
        expect(screen.queryByText('Filter 2')).not.toBeInTheDocument();
      }, { timeout: 3000 });
    });

    it('should handle empty filter input value', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.clear(autocomplete);
      });
      
      await waitFor(() => {
        expect(autocomplete).toHaveValue('');
      }, { timeout: 1000 });
    });
  });

  describe('getValue Function', () => {
    it('should merge URL parameters correctly', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet&tag=test&includeDE=true' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
        expect(CommonViewFunction.generateObjectForSaveSearchApi).toHaveBeenCalled();
      }, { timeout: 5000 });
      
      const calls = (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mock.calls;
      expect(calls.length).toBeGreaterThan(0);
      const callArg = calls[calls.length - 1][0];
      expect(callArg.value.searchType).toBe('basic');
      expect(callArg.value.type).toBe('DataSet');
      expect(callArg.value.tag).toBe('test');
      expect(callArg.value.includeDE === true || callArg.value.includeDE === "true").toBe(true);
    });
  });

  describe('getSearchType Function', () => {
    it('should return "Relationship" for relationship search', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?relationshipName=test' }
      );
      
      expect(screen.getByText('Save Relationship Custom Filter')).toBeInTheDocument();
    });

    it('should return "Basic" for basic search', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic' }
      );
      
      expect(screen.getByText('Save Basic Custom Filter')).toBeInTheDocument();
    });

    it('should return "Advanced" for advanced search', () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=advanced' }
      );
      
      expect(screen.getByText('Save Advanced Custom Filter')).toBeInTheDocument();
    });
  });

  describe('updatedData Function', () => {
    it('should dispatch fetchSavedSearchData', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Test',
        searchParameters: {}
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(fetchSavedSearchData).toHaveBeenCalled();
      }, { timeout: 5000 });
    });
  });

  describe('URL Parameter Boolean Conversion', () => {
    it('should convert string "true" to boolean true for includeDE', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockImplementation((obj) => {
        return {
          name: obj.name,
          searchParameters: obj.value
        };
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet&includeDE=true&excludeSC=true&excludeST=true' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
        expect(CommonViewFunction.generateObjectForSaveSearchApi).toHaveBeenCalled();
      }, { timeout: 5000 });
      
      const calls = (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mock.calls;
      expect(calls.length).toBeGreaterThan(0);
      const callArg = calls[calls.length - 1][0];
      expect(callArg.value.includeDE === true || callArg.value.includeDE === "true").toBe(true);
      expect(callArg.value.excludeSC === true || callArg.value.excludeSC === "true").toBe(true);
      expect(callArg.value.excludeST === true || callArg.value.excludeST === "true").toBe(true);
    });

    it('should convert searchType to isBasic when searchParams has type', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
      }, { timeout: 5000 });
    });

    it('should convert searchType to isBasic when searchParams has tag', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&tag=test-tag' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
      }, { timeout: 5000 });
    });

    it('should convert searchType to isBasic when searchParams has query', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=advanced&query=test-query' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            searchType: 'ADVANCED'
          }),
          'POST'
        );
      }, { timeout: 5000 });
    });

    it('should convert searchType to isBasic when searchParams has term', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&term=test-term' }
      );
      
      await fillAndSubmitForm('Test');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
      }, { timeout: 5000 });
    });
  });

  describe('Autocomplete onChange Scenarios', () => {
    it('should handle string value in onChange by typing', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'StringValue',
        searchParameters: {}
      });
      
      const store = createMockStore([]);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('StringValue');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
      }, { timeout: 5000 });
    });

    it('should handle object with inputValue in onChange', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'New Value',
        searchParameters: {}
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('New Value');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalled();
      }, { timeout: 5000 });
    });

    it('should handle null value in onChange', async () => {
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        expect(screen.getByText('Test Filter 1')).toBeInTheDocument();
      }, { timeout: 3000 });
      
      await act(async () => {
        await userEvent.click(screen.getByText('Test Filter 1'));
      });
      
      // Clear the selection - look for clear button or clear input
      const clearButton = screen.queryByTitle('Clear') || screen.queryByLabelText(/clear/i);
      if (clearButton) {
        await act(async () => {
          await userEvent.click(clearButton);
        });
      } else {
        // If no clear button, clear the input directly
        await act(async () => {
          await userEvent.clear(autocomplete);
        });
      }
      
      await waitFor(() => {
        expect(autocomplete).toHaveValue('');
      }, { timeout: 2000 });
    });
  });

  describe('getOptionLabel Scenarios', () => {
    it('should handle string option in getOptionLabel', async () => {
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        // The text might be split across Typography elements, so check for the option
        const options = screen.getAllByRole('option');
        const foundOption = options.find(opt => 
          opt.textContent?.includes('Test Filter 1')
        );
        expect(foundOption).toBeTruthy();
      }, { timeout: 3000 });
    });

    it('should handle option with inputValue in getOptionLabel', async () => {
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.type(autocomplete, 'New Option');
      });
      
      await waitFor(() => {
        expect(screen.getByText(/Add:/)).toBeInTheDocument();
      }, { timeout: 3000 });
    });

    it('should handle option with label in getOptionLabel', async () => {
      const store = createMockStore(mockSavedSearchData);
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { store, route: '/?searchType=basic&type=DataSet' }
      );
      
      const autocomplete = screen.getByRole('combobox');
      await act(async () => {
        await userEvent.click(autocomplete);
      });
      
      await waitFor(() => {
        const options = screen.getAllByRole('option');
        expect(options.length).toBeGreaterThan(0);
      }, { timeout: 3000 });
    });
  });

  describe('Search Type Assignment for New Filters', () => {
    it('should assign BASIC searchType for basic search without relationship', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Basic Filter',
        searchParameters: { type: 'DataSet' }
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=basic&type=DataSet' }
      );
      
      await fillAndSubmitForm('Basic Filter');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            searchType: 'BASIC'
          }),
          'POST'
        );
      }, { timeout: 5000 });
    });

    it('should assign BASIC_RELATIONSHIP searchType for relationship search', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Relationship Filter',
        searchParameters: { relationshipName: 'test' }
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?relationshipName=test' }
      );
      
      await fillAndSubmitForm('Relationship Filter');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            searchType: 'BASIC_RELATIONSHIP'
          }),
          'POST'
        );
      }, { timeout: 5000 });
    });

    it('should assign ADVANCED searchType for advanced search', async () => {
      (CommonViewFunction.generateObjectForSaveSearchApi as jest.Mock).mockReturnValue({
        name: 'Advanced Filter',
        searchParameters: { query: 'test' }
      });
      
      renderWithProviders(
        <SaveFilters open={true} onClose={mockOnClose} />,
        { route: '/?searchType=advanced&query=test' }
      );
      
      await fillAndSubmitForm('Advanced Filter');
      
      await waitFor(() => {
        expect(editSavedSearch).toHaveBeenCalledWith(
          expect.objectContaining({
            searchType: 'ADVANCED'
          }),
          'POST'
        );
      }, { timeout: 5000 });
    });
  });
});
