/**
 * Test utilities for the Atlas React dashboard
 */

import React, { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';

// Custom render function with providers
interface AllTheProvidersProps {
  children: React.ReactNode;
  withRouter?: boolean;
}

const AllTheProviders = ({ children, withRouter = true }: AllTheProvidersProps) => {
  if (withRouter) {
    return (
      <BrowserRouter>
        {children}
      </BrowserRouter>
    );
  }
  return <>{children}</>;
};

// Custom render function
const customRender = (
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'> & { withRouter?: boolean }
) => {
  const { withRouter = true, ...renderOptions } = options || {};
  return render(ui, { 
    wrapper: ({ children }) => <AllTheProviders withRouter={withRouter}>{children}</AllTheProviders>,
    ...renderOptions 
  });
};

// Re-export everything
export * from '@testing-library/react';

// Override render method
export { customRender as render };

// Mock data generators
export const mockEntity = {
  guid: 'mock-guid-123',
  typeName: 'DataSet',
  displayText: 'Mock Dataset',
  status: 'ACTIVE',
  attributes: {
    name: 'mock_dataset',
    qualifiedName: 'mock_dataset@cluster1',
    description: 'A mock dataset for testing'
  },
  relationshipAttributes: {},
  classifications: []
};

export const mockTypeDef = {
  name: 'DataSet',
  typeVersion: '1.0',
  description: 'A dataset type definition',
  attributeDefs: [
    {
      name: 'name',
      typeName: 'string',
      isOptional: false,
      cardinality: 'SINGLE'
    },
    {
      name: 'description',
      typeName: 'string',
      isOptional: true,
      cardinality: 'SINGLE'
    }
  ]
};

export const mockSearchResult = {
  queryText: 'test',
  entities: [mockEntity],
  searchParameters: {
    query: 'test',
    typeName: 'DataSet'
  }
};

// Mock API responses
export const mockApiResponses = {
  version: {
    Description: 'Metadata Management and Data Governance Platform over Hadoop',
    Revision: 'mock-revision',
    Version: '3.0.0-SNAPSHOT'
  },
  typeDefs: {
    entityDefs: [mockTypeDef],
    classificationDefs: [],
    relationshipDefs: []
  },
  searchBasic: mockSearchResult
};

// Test helpers
export const waitForLoadingToFinish = () => 
  new Promise(resolve => setTimeout(resolve, 0));

export const createMockStore = (initialState = {}) => {
  // Return a mock store implementation
  return {
    getState: () => initialState,
    dispatch: () => {},
    subscribe: () => {},
    replaceReducer: () => {}
  };
};