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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import UserDefinedProperties from '../PropertiesTab/UserDefinedProperties';

const theme = createTheme();

const mockDispatch = jest.fn();
const mockCreateEntity = jest.fn();
const mockEnrichEntityPayload = jest.fn();

jest.mock('@hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch
}));

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useParams: () => ({ guid: 'test-guid-123' })
}));

jest.mock('@api/apiMethods/entityFormApiMethod', () => ({
	createEntity: (...args: unknown[]) => mockCreateEntity(...args)
}));

jest.mock('@utils/entityPayloadEnrichmentUtils', () => ({
	enrichEntityPayloadForRelationshipSave: (...args: unknown[]) =>
		mockEnrichEntityPayload(...args)
}));

jest.mock('@redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: (guid: string) => ({
		type: 'detailPage/fetchDetailPageData',
		payload: guid
	})
}));

jest.mock('react-toastify', () => ({
	toast: {
		dismiss: jest.fn(),
		success: jest.fn(() => 'toast-id')
	}
}));

jest.mock('@utils/Utils', () => ({
	isEmpty: jest.fn(
		(val) =>
			val === null ||
			val === undefined ||
			val === '' ||
			(Array.isArray(val) && val.length === 0) ||
			(typeof val === 'object' &&
				val !== null &&
				Object.keys(val).length === 0)
	),
	serverError: jest.fn()
}));

jest.mock('@utils/Helper', () => ({
	cloneDeep: jest.fn((obj: unknown) => JSON.parse(JSON.stringify(obj)))
}));

jest.mock('@components/SkeletonLoader', () => ({
	__esModule: true,
	default: () => <div data-testid="skeleton-loader">Loading</div>
}));

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick, disabled }: any) => (
		<button type="button" onClick={onClick} disabled={disabled}>
			{children}
		</button>
	),
	AccordionDetails: ({ children }: any) => <div>{children}</div>,
	AccordionSummary: ({ children, onChange }: any) => (
		<div
			onClick={() => onChange?.({}, true)}
			onKeyDown={() => {}}
			role="button"
			tabIndex={0}
		>
			{children}
		</div>
	),
	TextArea: () => null
}));

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
	<ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('UserDefinedProperties', () => {
	const mockEntity = {
		guid: 'test-guid-123',
		typeName: 'hive_column',
		attributes: {
			name: 'col1',
			qualifiedName: 'db.table.col1@cm'
		},
		relationshipAttributes: {
			meanings: [{ guid: 'term-1' }]
		}
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockEnrichEntityPayload.mockResolvedValue(undefined);
		mockCreateEntity.mockResolvedValue({});
		mockDispatch.mockReturnValue({ unwrap: jest.fn() });
	});

	it('renders existing custom attributes in read mode', () => {
		render(
			<TestWrapper>
				<UserDefinedProperties
					loading={false}
					customAttributes={{ demo: 'demo1' }}
					entity={mockEntity}
				/>
			</TestWrapper>
		);

		expect(screen.getByText('User-defined properties')).toBeTruthy();
		expect(screen.getByText('demo')).toBeTruthy();
		expect(screen.getByText('demo1')).toBeTruthy();
	});

	it('enriches entity payload before createEntity on save', async () => {
		render(
			<TestWrapper>
				<UserDefinedProperties
					loading={false}
					customAttributes={{}}
					entity={mockEntity}
				/>
			</TestWrapper>
		);

		fireEvent.click(screen.getByRole('button', { name: 'Add' }));

		const keyInput = screen.getByPlaceholderText('key');
		const valueInput = screen.getByPlaceholderText('value');
		fireEvent.change(keyInput, { target: { value: 'demo' } });
		fireEvent.change(valueInput, { target: { value: 'demo1' } });

		fireEvent.click(screen.getByRole('button', { name: 'Save' }));

		await waitFor(() => {
			expect(mockEnrichEntityPayload).toHaveBeenCalled();
		});

		await waitFor(() => {
			expect(mockCreateEntity).toHaveBeenCalledWith({
				entity: expect.objectContaining({
					guid: 'test-guid-123',
					customAttributes: { demo: 'demo1' }
				})
			});
		});

		const enrichCallOrder =
			mockEnrichEntityPayload.mock.invocationCallOrder[0];
		const createCallOrder = mockCreateEntity.mock.invocationCallOrder[0];
		expect(enrichCallOrder).toBeLessThan(createCallOrder);
	});
});
