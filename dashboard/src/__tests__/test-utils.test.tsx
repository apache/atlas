/**
 * Tests for test utilities
 */

import React from 'react';
import { screen } from '@testing-library/react';
import { render, mockEntity, mockTypeDef, waitForLoadingToFinish } from '../utils/test-utils';

// Simple test component
const TestComponent = ({ text }: { text: string }) => (
  <div data-testid="test-component">{text}</div>
);

describe('Test Utils', () => {
  describe('render', () => {
    it('renders component with all providers', () => {
      render(<TestComponent text="Hello Test" />);
      
      expect(screen.getByTestId('test-component')).toBeTruthy();
      expect(screen.getByText('Hello Test')).toBeTruthy();
    });
  });

  describe('mockEntity', () => {
    it('has required properties', () => {
      expect(mockEntity).toHaveProperty('guid');
      expect(mockEntity).toHaveProperty('typeName');
      expect(mockEntity).toHaveProperty('displayText');
      expect(mockEntity).toHaveProperty('status');
      expect(mockEntity).toHaveProperty('attributes');
      expect(mockEntity.guid).toBe('mock-guid-123');
      expect(mockEntity.typeName).toBe('DataSet');
    });
  });

  describe('mockTypeDef', () => {
    it('has required properties', () => {
      expect(mockTypeDef).toHaveProperty('name');
      expect(mockTypeDef).toHaveProperty('typeVersion');
      expect(mockTypeDef).toHaveProperty('attributeDefs');
      expect(mockTypeDef.name).toBe('DataSet');
      expect(Array.isArray(mockTypeDef.attributeDefs)).toBe(true);
    });
  });

  describe('waitForLoadingToFinish', () => {
    it('resolves after a timeout', async () => {
      const start = Date.now();
      await waitForLoadingToFinish();
      const end = Date.now();
      
      // Should complete almost immediately
      expect(end - start).toBeLessThan(50);
    });
  });
});