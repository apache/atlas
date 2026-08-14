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