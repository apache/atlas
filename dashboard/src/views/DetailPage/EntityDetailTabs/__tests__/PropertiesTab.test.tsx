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
 * Unit tests for PropertiesTab component
 */

import React from 'react';
import { render, screen } from '@utils/test-utils';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import PropertiesTab from '../PropertiesTab/PropertiesTab';

const theme = createTheme();

// Mock child components
jest.mock('../AttributeProperties', () => ({
	__esModule: true,
	default: ({ propertiesName }: any) => (
		<div data-testid={`${propertiesName.toLowerCase()}-properties`}>{propertiesName} Properties</div>
	)
}));

jest.mock('../PropertiesTab/UserDefinedProperties', () => ({
	__esModule: true,
	default: () => <div data-testid="user-defined-properties">User Defined Properties</div>
}));

jest.mock('../PropertiesTab/Labels', () => ({
	__esModule: true,
	default: () => <div data-testid="labels">Labels</div>
}));

jest.mock('../PropertiesTab/BMAttributes', () => ({
	__esModule: true,
	default: () => <div data-testid="bm-attributes">Business Metadata Attributes</div>
}));

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
	<ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('PropertiesTab', () => {
	const mockEntity = {
		guid: 'test-guid',
		typeName: 'DataSet',
		attributes: {
			name: 'Test Entity',
			description: 'Test Description'
		},
		customAttributes: {
			custom1: 'value1'
		},
		labels: ['label1', 'label2'],
		businessAttributes: {
			bm1: 'bm-value1'
		}
	};

	const mockReferredEntities = {};

	it('should render PropertiesTab component', () => {
		render(
			<TestWrapper>
				<PropertiesTab entity={mockEntity} referredEntities={mockReferredEntities} loading={false} />
			</TestWrapper>
		);

		expect(screen.getByTestId('technical-properties')).toBeTruthy();
	});

	it('should display technical properties', () => {
		render(
			<TestWrapper>
				<PropertiesTab entity={mockEntity} referredEntities={mockReferredEntities} loading={false} />
			</TestWrapper>
		);

		expect(screen.getByText('Technical Properties')).toBeTruthy();
	});

	it('should display user-defined properties', () => {
		render(
			<TestWrapper>
				<PropertiesTab entity={mockEntity} referredEntities={mockReferredEntities} loading={false} />
			</TestWrapper>
		);

		expect(screen.getByTestId('user-defined-properties')).toBeTruthy();
	});

	it('should display labels', () => {
		render(
			<TestWrapper>
				<PropertiesTab entity={mockEntity} referredEntities={mockReferredEntities} loading={false} />
			</TestWrapper>
		);

		expect(screen.getByTestId('labels')).toBeTruthy();
	});

	it('should display business metadata attributes', () => {
		render(
			<TestWrapper>
				<PropertiesTab entity={mockEntity} referredEntities={mockReferredEntities} loading={false} />
			</TestWrapper>
		);

		expect(screen.getByTestId('bm-attributes')).toBeTruthy();
	});

	it('should handle loading state', () => {
		render(
			<TestWrapper>
				<PropertiesTab entity={mockEntity} referredEntities={mockReferredEntities} loading={true} />
			</TestWrapper>
		);

		// Component should render even when loading
		expect(screen.getByTestId('technical-properties')).toBeTruthy();
	});

	it('should handle empty entity', () => {
		render(
			<TestWrapper>
				<PropertiesTab entity={null} referredEntities={mockReferredEntities} loading={false} />
			</TestWrapper>
		);

		// Should handle gracefully
		expect(screen.getByTestId('technical-properties')).toBeTruthy();
	});

	it('should handle entity without custom attributes', () => {
		const entityWithoutCustom = {
			...mockEntity,
			customAttributes: {}
		};

		render(
			<TestWrapper>
				<PropertiesTab entity={entityWithoutCustom} referredEntities={mockReferredEntities} loading={false} />
			</TestWrapper>
		);

		expect(screen.getByTestId('user-defined-properties')).toBeTruthy();
	});

	it('should handle entity without labels', () => {
		const entityWithoutLabels = {
			...mockEntity,
			labels: []
		};

		render(
			<TestWrapper>
				<PropertiesTab entity={entityWithoutLabels} referredEntities={mockReferredEntities} loading={false} />
			</TestWrapper>
		);

		expect(screen.getByTestId('labels')).toBeTruthy();
	});
});

