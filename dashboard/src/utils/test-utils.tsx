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

/** Test utilities for the Atlas React dashboard */

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