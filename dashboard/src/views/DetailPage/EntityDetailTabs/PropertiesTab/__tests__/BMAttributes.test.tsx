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
import '@testing-library/jest-dom';
import BMAttributes from '../BMAttributes';
import { ThemeProvider, createTheme } from '@mui/material/styles';

const theme = createTheme();

// Mock dependencies
const mockDispatch = jest.fn();
jest.mock('@hooks/reducerHook', () => ({
  useAppDispatch: () => mockDispatch,
  useAppSelector: jest.fn((selector) => {
    const state = {
      entity: {
        entityData: {
          entityDefs: [
            {
              name: 'DataSet',
              businessAttributeDefs: {
                'Group1': [
                  { name: 'attr1', typeName: 'string' },
                  { name: 'attr2', typeName: 'int' }
                ]
              }
            }
          ]
        }
      },
      businessMetaData: {
        businessMetaData: {
          businessMetadataDefs: [
            {
              name: 'Group1',
              attributeDefs: [
                { name: 'attr1', typeName: 'string' },
                { name: 'attr2', typeName: 'int' }
              ]
            }
          ]
        }
      }
    };
    return selector(state);
  })
}));

jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useParams: () => ({ guid: 'test-guid-123' })
}));

const mockGetEntityBusinessMetadata = jest.fn();
jest.mock('@api/apiMethods/detailpageApiMethod', () => ({
  getEntityBusinessMetadata: (...args: any[]) => mockGetEntityBusinessMetadata(...args)
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

// Mock BMAttributesFields to avoid complex form input mocks unless needed
jest.mock('../BMAttributesFields', () => {
  return function MockBMAttributesFields(props: any) {
    return <div data-testid="bm-fields-mock">{props.obj?.name}</div>;
  };
});

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('BMAttributes Component', () => {
  const defaultProps = {
    loading: false,
    bmAttributes: {
      'Group1': {
        'attr1': 'value1',
        'attr2': 100
      }
    },
    entity: { guid: 'test-guid-123', status: 'ACTIVE', typeName: 'DataSet' }
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders existing business metadata correctly', () => {
    render(<TestWrapper><BMAttributes {...defaultProps} /></TestWrapper>);
    
    expect(screen.getByText('Business Metadata')).toBeInTheDocument();
    expect(screen.getByText('Group1')).toBeInTheDocument();
    expect(screen.getByText('attr1 (string)')).toBeInTheDocument();
    expect(screen.getByText('attr2 (int)')).toBeInTheDocument();
    // BMAttributes renders HTML for string values
    expect(screen.getByText('value1')).toBeInTheDocument();
    expect(screen.getByText('100')).toBeInTheDocument();
  });

  it('shows empty state message when empty', () => {
    render(<TestWrapper><BMAttributes loading={false} bmAttributes={{}} entity={{ status: 'ACTIVE', typeName: 'DataSet' }} /></TestWrapper>);
    
    expect(screen.getByText(/No properties have been created yet/i)).toBeInTheDocument();
  });

  it('does not allow editing if entity is deleted', () => {
    render(<TestWrapper><BMAttributes loading={false} bmAttributes={{}} entity={{ status: 'DELETED', typeName: 'DataSet' }} /></TestWrapper>);
    
    expect(screen.getByText(/No properties have been created yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/To add a property, click/i)).not.toBeInTheDocument();
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
  });

  it('does not allow editing if entity is purged', () => {
    render(<TestWrapper><BMAttributes loading={false} bmAttributes={{}} entity={{ status: 'PURGED', typeName: 'DataSet' }} /></TestWrapper>);
    
    expect(screen.getByText(/No properties have been created yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/To add a property, click/i)).not.toBeInTheDocument();
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
  });

  it('hides edit button for deleted entity even when business metadata exists', () => {
    render(<TestWrapper><BMAttributes {...defaultProps} entity={{ ...defaultProps.entity, status: 'DELETED' }} /></TestWrapper>);
    
    expect(screen.getByText('value1')).toBeInTheDocument();
    expect(screen.queryByText('Edit')).not.toBeInTheDocument();
  });

  it('hides edit button for purged entity even when business metadata exists', () => {
    render(<TestWrapper><BMAttributes {...defaultProps} entity={{ ...defaultProps.entity, status: 'PURGED' }} /></TestWrapper>);
    
    expect(screen.getByText('value1')).toBeInTheDocument();
    expect(screen.queryByText('Edit')).not.toBeInTheDocument();
  });

  it('switches to edit mode on Edit button click', () => {
    render(<TestWrapper><BMAttributes {...defaultProps} /></TestWrapper>);
    
    const editBtn = screen.getByText('Edit').closest('button')!;
    fireEvent.click(editBtn);

    expect(screen.getAllByTestId('bm-fields-mock')).toHaveLength(2);
    expect(screen.getByText('attr1')).toBeInTheDocument();
    expect(screen.getByText('attr2')).toBeInTheDocument();
  });

  it('adds new attribute dynamically', async () => {
    render(<TestWrapper><BMAttributes {...defaultProps} /></TestWrapper>);
    
    fireEvent.click(screen.getByText('Edit').closest('button')!);
    
    const addAttrBtn = screen.getByRole('button', { name: /Add New Attribute/i });
    fireEvent.click(addAttrBtn);
    
    // There should be 3 items now
    const removeBtns = screen.getAllByTestId('RemoveOutlinedIcon');
    expect(removeBtns).toHaveLength(3);
  });

  it('submits form successfully', async () => {
    mockGetEntityBusinessMetadata.mockResolvedValueOnce({ data: {} });
    
    render(<TestWrapper><BMAttributes {...defaultProps} /></TestWrapper>);
    
    fireEvent.click(screen.getByText('Edit').closest('button')!);
    
    const saveBtn = screen.getAllByRole('button', { name: /save/i })[0];
    await act(async () => { fireEvent.submit(saveBtn.closest('form')!); });
    
    await waitFor(() => {
      expect(mockGetEntityBusinessMetadata).toHaveBeenCalledWith('test-guid-123', expect.any(Object));
      expect(mockDispatch).toHaveBeenCalled();
    });
  });

  it('handles API failure on save gracefully', async () => {
    mockGetEntityBusinessMetadata.mockRejectedValueOnce(new Error('Network error'));
    
    render(<TestWrapper><BMAttributes {...defaultProps} /></TestWrapper>);
    
    fireEvent.click(screen.getByText('Edit').closest('button')!);
    
    const saveBtn = screen.getAllByRole('button', { name: /save/i })[0];
    await act(async () => { fireEvent.submit(saveBtn.closest('form')!); });
    
    await waitFor(() => {
      expect(mockGetEntityBusinessMetadata).toHaveBeenCalled();
    });
    
    const { serverError } = require('@utils/Utils');
    expect(serverError).toHaveBeenCalled();
  });

  it('hides Add button while loading', () => {
    render(<TestWrapper><BMAttributes loading={true} bmAttributes={{}} entity={{ status: 'ACTIVE', typeName: 'DataSet' }} /></TestWrapper>);
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
  });

  it('hides Edit button while loading even when business metadata exists', () => {
    render(<TestWrapper><BMAttributes {...defaultProps} loading={true} entity={{ ...defaultProps.entity, status: 'ACTIVE' }} /></TestWrapper>);
    expect(screen.queryByText('Edit')).not.toBeInTheDocument();
  });

});
