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
import { render, screen, fireEvent, waitFor, act } from '@utils/test-utils';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import Labels from '../Labels';
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

const mockGetLabels = jest.fn();
const mockGetGlobalSearchResult = jest.fn();
jest.mock('@api/apiMethods/detailpageApiMethod', () => ({
  getLabels: (...args: any[]) => mockGetLabels(...args)
}));
jest.mock('@api/apiMethods/searchApiMethod', () => ({
  getGlobalSearchResult: (...args: any[]) => mockGetGlobalSearchResult(...args)
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

describe('Labels Component', () => {
  const defaultProps = {
    loading: false,
    labels: ['Label1', 'Label2'],
    entity: { status: 'ACTIVE' }
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders existing labels correctly', () => {
    render(<TestWrapper><Labels {...defaultProps} /></TestWrapper>);
    
    // Labels are shown in an accordion that is expanded by default since labels exist
    expect(screen.getByText('Labels')).toBeInTheDocument();
    expect(screen.getByText('Label1')).toBeInTheDocument();
    expect(screen.getByText('Label2')).toBeInTheDocument();
  });

  it('shows no labels message when empty', () => {
    render(<TestWrapper><Labels loading={false} labels={[]} entity={{ status: 'ACTIVE' }} /></TestWrapper>);
    
    expect(screen.getByText(/No labels have been created yet/i)).toBeInTheDocument();
  });

  it('does not allow editing if entity is deleted', () => {
    render(<TestWrapper><Labels loading={false} labels={[]} entity={{ status: 'DELETED' }} /></TestWrapper>);
    
    expect(screen.getByText(/No labels have been created yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/To add a labels, click/i)).not.toBeInTheDocument();
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
  });

  it('allows clicking edit to show autocomplete form', async () => {
    render(<TestWrapper><Labels {...defaultProps} /></TestWrapper>);
    
    const editBtn = screen.getByText('Edit').closest('button')!;
    fireEvent.click(editBtn);

    expect(await screen.findByPlaceholderText('Select Label')).toBeInTheDocument();
  });

  it('allows clicking "here" to add label when empty', async () => {
    render(<TestWrapper><Labels loading={false} labels={[]} entity={{ status: 'ACTIVE' }} /></TestWrapper>);
    
    const hereText = screen.getByText('here');
    fireEvent.click(hereText);

    expect(await screen.findByPlaceholderText('Select Label')).toBeInTheDocument();
  });

  it('calls API and dispatches action on save', async () => {
    mockGetLabels.mockResolvedValueOnce({ data: {} });
    
    render(<TestWrapper><Labels {...defaultProps} /></TestWrapper>);
    
    // Click Edit
    fireEvent.click(screen.getByText('Edit').closest('button')!);
    
    // Since autocomplete already has default value, let's just save
    const saveBtn = screen.getAllByRole('button', { name: /save/i })[0];
    await act(async () => { fireEvent.submit(saveBtn.closest('form')!); });
    
    await waitFor(() => {
      expect(mockGetLabels).toHaveBeenCalledWith('test-guid-123', ['Label1', 'Label2']);
      expect(mockDispatch).toHaveBeenCalled();
    });
  });

  it('handles API failure on save gracefully', async () => {
    mockGetLabels.mockRejectedValueOnce(new Error('Network error'));
    
    render(<TestWrapper><Labels {...defaultProps} /></TestWrapper>);
    
    // Click Edit
    fireEvent.click(screen.getByText('Edit').closest('button')!);
    
    // Save
    const saveBtn = screen.getAllByRole('button', { name: /save/i })[0];
    await act(async () => { fireEvent.submit(saveBtn.closest('form')!); });
    
    await waitFor(() => {
      expect(mockGetLabels).toHaveBeenCalled();
    });
    
    // Error is handled via serverError util mock
    const { serverError } = require('@utils/Utils');
    expect(serverError).toHaveBeenCalled();
  });
  
  it('fetches label suggestions on open', async () => {
    mockGetGlobalSearchResult.mockResolvedValueOnce({ data: { suggestions: ['Label3', 'Label4'] } });
    
    render(<TestWrapper><Labels {...defaultProps} /></TestWrapper>);
    
    // Click Edit
    fireEvent.click(screen.getByText('Edit').closest('button')!);
    
    const input = await screen.findByPlaceholderText('Select Label');
    fireEvent.mouseDown(input);
    
    await waitFor(() => {
      expect(mockGetGlobalSearchResult).toHaveBeenCalledWith('suggestions', expect.objectContaining({ params: { fieldName: '__labels' } }));
    });
  });
});
