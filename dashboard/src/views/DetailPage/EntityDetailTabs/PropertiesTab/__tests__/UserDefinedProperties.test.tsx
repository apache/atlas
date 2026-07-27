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

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@utils/test-utils';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import UserDefinedProperties from '../UserDefinedProperties';
import { ThemeProvider, createTheme } from '@mui/material/styles';

const theme = createTheme();

// Mock dependencies
const mockDispatch = jest.fn();
jest.mock('@hooks/reducerHook', () => ({
  useAppDispatch: () => mockDispatch
}));

jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useParams: () => ({ guid: 'test-guid-123' })
}));

const mockCreateEntity = jest.fn();
jest.mock('@api/apiMethods/entityFormApiMethod', () => ({
  createEntity: (...args: any[]) => mockCreateEntity(...args)
}));

jest.mock('@utils/entityPayloadEnrichmentUtils', () => ({
  enrichEntityPayloadForRelationshipSave: jest.fn(async (entity) => entity)
}));

jest.mock('react-toastify', () => ({
  toast: {
    dismiss: jest.fn(),
    success: jest.fn(() => 'toast-id'),
    error: jest.fn(() => 'toast-id')
  }
}));

jest.mock('@utils/Utils', () => ({
  ...jest.requireActual('@utils/Utils'),
  serverError: jest.fn()
}));

jest.mock('@redux/slice/detailPageSlice', () => ({
  fetchDetailPageData: jest.fn((guid: string) => ({ type: 'fetchDetailPageData', payload: guid }))
}));

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('UserDefinedProperties Component', () => {
  const defaultProps = {
    loading: false,
    customAttributes: { key1: 'value1', key2: 'value2' },
    entity: { guid: 'test-guid-123', status: 'ACTIVE', customAttributes: {} }
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders existing properties correctly', () => {
    render(<TestWrapper><UserDefinedProperties {...defaultProps} /></TestWrapper>);
    
    expect(screen.getByText('User-defined properties')).toBeInTheDocument();
    expect(screen.getByText('key1')).toBeInTheDocument();
    expect(screen.getByText('value1')).toBeInTheDocument();
    expect(screen.getByText('key2')).toBeInTheDocument();
    expect(screen.getByText('value2')).toBeInTheDocument();
  });

  it('shows empty state message when empty', () => {
    render(<TestWrapper><UserDefinedProperties loading={false} customAttributes={{}} entity={{ status: 'ACTIVE' }} /></TestWrapper>);
    
    expect(screen.getByText(/No properties have been created yet/i)).toBeInTheDocument();
  });

  it('does not allow editing if entity is deleted', () => {
    render(<TestWrapper><UserDefinedProperties loading={false} customAttributes={{}} entity={{ status: 'DELETED' }} /></TestWrapper>);
    
    expect(screen.getByText(/No properties have been created yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/To add a property,click/i)).not.toBeInTheDocument();
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
  });

  it('switches to edit mode on Edit button click', () => {
    render(<TestWrapper><UserDefinedProperties {...defaultProps} /></TestWrapper>);
    
    const editBtn = screen.getByRole('button', { name: /edit/i });
    fireEvent.click(editBtn);

    expect(screen.getByDisplayValue('key1')).toBeInTheDocument();
    expect(screen.getByDisplayValue('value1')).toBeInTheDocument();
  });

  it('adds and removes fields dynamically', async () => {
    render(<TestWrapper><UserDefinedProperties {...defaultProps} /></TestWrapper>);
    
    // Edit mode
    fireEvent.click(screen.getAllByRole('button', { name: /edit/i })[0]);
    
    // Should have 2 inputs initially for keys
    let keyInputs = screen.getAllByPlaceholderText('key');
    expect(keyInputs).toHaveLength(2);
    
    // Add new field
    const addBtns = screen.getAllByTestId('AddOutlinedIcon');
    fireEvent.click(addBtns[0]);
    
    keyInputs = screen.getAllByPlaceholderText('key');
    expect(keyInputs).toHaveLength(3);
    
    // Remove field
    const removeBtns = screen.getAllByTestId('RemoveOutlinedIcon');
    fireEvent.click(removeBtns[0]); // Removing first
    
    keyInputs = screen.getAllByPlaceholderText('key');
    expect(keyInputs).toHaveLength(2);
  });

  it('validates unique keys', async () => {
    render(<TestWrapper><UserDefinedProperties {...defaultProps} /></TestWrapper>);
    
    fireEvent.click(screen.getAllByRole('button', { name: /edit/i })[0]);
    
    const keyInputs = screen.getAllByPlaceholderText('key');
    // Change second key to 'key1' to cause duplicate
    fireEvent.change(keyInputs[1], { target: { value: 'key1' } });
    
    // Form submission
    const saveBtn = screen.getAllByRole('button', { name: /save/i })[0];
    fireEvent.click(saveBtn);
    
    await waitFor(() => {
      expect(screen.getByText('Key must be unique')).toBeInTheDocument();
      expect(mockCreateEntity).not.toHaveBeenCalled();
    });
  });

  it('submits form successfully', async () => {
    mockCreateEntity.mockResolvedValueOnce({ data: {} });
    
    render(<TestWrapper><UserDefinedProperties {...defaultProps} /></TestWrapper>);
    
    fireEvent.click(screen.getAllByRole('button', { name: /edit/i })[0]);
    
    const saveBtn = screen.getAllByRole('button', { name: /save/i })[0];
    fireEvent.click(saveBtn);
    
    await waitFor(() => {
      expect(mockCreateEntity).toHaveBeenCalledWith({
        entity: expect.objectContaining({
          customAttributes: { key1: 'value1', key2: 'value2' }
        })
      });
      expect(mockDispatch).toHaveBeenCalled();
    });
  });

  it('handles API failure on save gracefully', async () => {
    mockCreateEntity.mockRejectedValueOnce(new Error('Network error'));
    
    render(<TestWrapper><UserDefinedProperties {...defaultProps} /></TestWrapper>);
    
    fireEvent.click(screen.getAllByRole('button', { name: /edit/i })[0]);
    
    const saveBtn = screen.getAllByRole('button', { name: /save/i })[0];
    fireEvent.click(saveBtn);
    
    await waitFor(() => {
      expect(mockCreateEntity).toHaveBeenCalled();
    });
    
    const { serverError } = require('@utils/Utils');
    expect(serverError).toHaveBeenCalled();
  });
});
