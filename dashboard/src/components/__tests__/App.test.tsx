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