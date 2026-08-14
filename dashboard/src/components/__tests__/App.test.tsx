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


/**
 * Unit tests for App component
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import App from '../../App';

// Stub Router to avoid loading nested ESM/axios imports in tests
jest.mock('../../views/Router', () => () => <div data-testid="router" />);

// Mock the Router dependencies
jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  BrowserRouter: ({ children }: { children: React.ReactNode }) => <div data-testid="router">{children}</div>,
}));

// Mock app dispatch hook and session slice used in App
jest.mock('@hooks/reducerHook', () => ({
  useAppDispatch: () => jest.fn()
}));
jest.mock('@redux/slice/sessionSlice', () => ({
  fetchSessionData: jest.fn(() => ({ type: 'session/fetch' }))
}));

// Mock Redux dependencies
jest.mock('react-redux', () => ({
  Provider: ({ children }: { children: React.ReactNode }) => <div data-testid="redux-provider">{children}</div>,
}), { virtual: true } as any);

// No store import in App.tsx; no need to mock store

describe('App', () => {
  beforeEach(() => {
    // Clear any previous mocks
    jest.clearAllMocks();
  });

  it('renders without crashing', () => {
    expect(() => render(<App />)).not.toThrow();
  });

  it('renders the main app structure', () => {
    render(<App />);
    
    // Check that the Router wrapper is present
    expect(screen.getByTestId('router')).toBeTruthy();
  });

  it('renders the app container', () => {
    const { container } = render(<App />);
    
    // Check that the main app container exists
    expect(container.firstChild).toBeTruthy();
  });
});