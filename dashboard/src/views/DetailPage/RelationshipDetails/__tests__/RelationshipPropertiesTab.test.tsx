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
 * Comprehensive unit tests for RelationshipPropertiesTab component
 * Target: 100% coverage for statements, branches, functions, and lines
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import RelationshipPropertiesTab from '../RelationshipPropertiesTab';

// Mock dependencies
jest.mock('@components/commonComponents', () => ({
	getValues: (value: any, ...args: any[]) => {
		if (Array.isArray(value)) {
			return value.join(', ');
		}
		if (typeof value === 'object' && value !== null) {
			return JSON.stringify(value);
		}
		return String(value);
	}
}));

jest.mock('@components/SkeletonLoader', () => ({
	__esModule: true,
	default: ({ count, animation }: any) => (
		<div data-testid="skeleton-loader">Loading...</div>
	)
}));

jest.mock('@utils/Utils', () => ({
	isArray: (value: any) => Array.isArray(value),
	isEmpty: (value: any) => {
		if (value === null || value === undefined) return true;
		if (typeof value === 'string') return value.trim() === '';
		if (Array.isArray(value)) return value.length === 0;
		if (typeof value === 'object') return Object.keys(value).length === 0;
		return false;
	},
	pick: (obj: any, keys: string[]) => {
		const result: any = {};
		keys.forEach(key => {
			if (obj && key in obj) {
				result[key] = obj[key];
			}
		});
		return result;
	}
}));

jest.mock('@components/muiComponents', () => ({
	Accordion: ({ children, defaultExpanded }: any) => (
		<div data-testid="accordion" data-default-expanded={defaultExpanded}>
			{children}
		</div>
	),
	AccordionDetails: ({ children }: any) => (
		<div data-testid="accordion-details">{children}</div>
	),
	AccordionSummary: ({ children, 'aria-controls': ariaControls, id }: any) => (
		<div data-testid="accordion-summary" aria-controls={ariaControls} id={id}>
			{children}
		</div>
	)
}));

describe('RelationshipPropertiesTab - 100% Coverage', () => {
	describe('Component Rendering', () => {
		test('renders component successfully', () => {
			const entity = {
				guid: 'test-guid',
				label: 'test-label',
				status: 'ACTIVE'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText('Technical Properties')).toBeInTheDocument();
			expect(screen.getByText('Relationship Properties')).toBeInTheDocument();
			expect(screen.getByText('End1')).toBeInTheDocument();
			expect(screen.getByText('End2')).toBeInTheDocument();
		});

		test('renders all accordions', () => {
			const entity = {
				guid: 'test-guid'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordions = screen.getAllByTestId('accordion');
			expect(accordions).toHaveLength(4); // Technical, Relationship, End1, End2
		});
	});

	describe('Loading State', () => {
		test('shows skeleton loader when loading is true', () => {
			render(<RelationshipPropertiesTab entity={{}} loading={true} />);

			expect(screen.getByTestId('skeleton-loader')).toBeInTheDocument();
		});

		test('shows skeleton loader when loading is undefined', () => {
			render(<RelationshipPropertiesTab entity={{}} loading={undefined} />);

			expect(screen.getByTestId('skeleton-loader')).toBeInTheDocument();
		});

		test('does not show skeleton loader when loading is false', () => {
			render(<RelationshipPropertiesTab entity={{}} loading={false} />);

			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument();
		});
	});

	describe('Technical Properties Section', () => {
		test('renders technical properties with all fields', () => {
			const entity = {
				createTime: 1640995200000,
				createdBy: 'test-user',
				blockedPropagatedClassifications: ['class1', 'class2'],
				guid: 'test-guid-123',
				label: 'test-label',
				propagateTags: 'BOTH',
				propagatedClassifications: ['prop1'],
				provenanceType: 'NATIVE',
				status: 'ACTIVE',
				updateTime: 1640995300000,
				updatedBy: 'update-user',
				version: 1
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.getAllByText(/label/).length).toBeGreaterThan(0);
			expect(screen.getByText(/status/)).toBeInTheDocument();
			expect(screen.getByText(/version/)).toBeInTheDocument();
		});

		test('filters out empty properties', () => {
			const entity = {
				guid: 'test-guid',
				label: '',
				status: null,
				version: undefined,
				createdBy: 'test-user'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.getByText(/createdBy/)).toBeInTheDocument();
			expect(screen.queryByText(/label/)).not.toBeInTheDocument();
			expect(screen.queryByText(/status/)).not.toBeInTheDocument();
		});

		test('shows "No Record Found" when all properties are empty', () => {
			const entity = {
				guid: '',
				label: null,
				status: undefined
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[0]).toHaveTextContent('No Record Found');
		});

		test('shows "No Record Found" when entity is empty object', () => {
			render(<RelationshipPropertiesTab entity={{}} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[0]).toHaveTextContent('No Record Found');
		});

		test('renders array properties with count', () => {
			const entity = {
				guid: 'test-guid',
				blockedPropagatedClassifications: ['class1', 'class2', 'class3']
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText(/blockedPropagatedClassifications \(3\)/)).toBeInTheDocument();
		});

		test('renders non-array properties without count', () => {
			const entity = {
				guid: 'test-guid',
				label: 'test-label'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText('guid')).toBeInTheDocument();
			expect(screen.queryByText(/guid \(/)).not.toBeInTheDocument();
		});

		test('sorts properties alphabetically', () => {
			const entity = {
				version: 1,
				guid: 'test-guid',
				createdBy: 'user',
				status: 'ACTIVE'
			};

			const { container } = render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const propertyNames = Array.from(
				container.querySelectorAll('[style*="fontWeight: 600"]')
			).map(el => el.textContent);

			// Should be sorted alphabetically
			const sortedNames = [...propertyNames].sort();
			expect(propertyNames).toEqual(sortedNames);
		});
	});

	describe('Relationship Properties Section', () => {
		test('always shows "No Record found!" message', () => {
			const entity = {
				guid: 'test-guid'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[1]).toHaveTextContent('No Record found!');
		});
	});

	describe('End1 Section', () => {
		test('renders End1 properties when available', () => {
			const entity = {
				guid: 'test-guid',
				end1: {
					guid: 'end1-guid',
					typeName: 'End1Type',
					uniqueAttributes: { qualifiedName: 'end1@cluster' }
				}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText('End1')).toBeInTheDocument();
			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.getByText(/typeName/)).toBeInTheDocument();
		});

		test('shows "No Record Found" when End1 is empty', () => {
			const entity = {
				guid: 'test-guid',
				end1: {}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[2]).toHaveTextContent('No Record Found');
		});

		test('shows "No Record Found" when End1 is undefined', () => {
			const entity = {
				guid: 'test-guid'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[2]).toHaveTextContent('No Record Found');
		});

		test('renders End1 array properties with count', () => {
			const entity = {
				guid: 'test-guid',
				end1: {
					classifications: ['class1', 'class2']
				}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText(/classifications \(2\)/)).toBeInTheDocument();
		});

		test('sorts End1 properties alphabetically', () => {
			const entity = {
				guid: 'test-guid',
				end1: {
					typeName: 'Type',
					guid: 'end1-guid',
					attributes: {}
				}
			};

			const { container } = render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const propertyNames = Array.from(
				container.querySelectorAll('[style*="fontWeight: 600"]')
			)
				.map(el => el.textContent)
				.filter(text => text && (text.includes('guid') || text.includes('typeName') || text.includes('attributes')));

			const sortedNames = [...propertyNames].sort();
			expect(propertyNames).toEqual(sortedNames);
		});
	});

	describe('End2 Section', () => {
		test('renders End2 properties when available', () => {
			const entity = {
				guid: 'test-guid',
				end2: {
					guid: 'end2-guid',
					typeName: 'End2Type',
					uniqueAttributes: { qualifiedName: 'end2@cluster' }
				}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText('End2')).toBeInTheDocument();
		});

		test('shows "No Record Found" when End2 is empty', () => {
			const entity = {
				guid: 'test-guid',
				end2: {}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[3]).toHaveTextContent('No Record Found');
		});

		test('shows "No Record Found" when End2 is undefined', () => {
			const entity = {
				guid: 'test-guid'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[3]).toHaveTextContent('No Record Found');
		});

		test('renders End2 array properties with count', () => {
			const entity = {
				guid: 'test-guid',
				end2: {
					classifications: ['class1', 'class2', 'class3']
				}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText(/classifications \(3\)/)).toBeInTheDocument();
		});

		test('sorts End2 properties alphabetically', () => {
			const entity = {
				guid: 'test-guid',
				end2: {
					typeName: 'Type',
					guid: 'end2-guid',
					attributes: {}
				}
			};

			const { container } = render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const propertyNames = Array.from(
				container.querySelectorAll('[style*="fontWeight: 600"]')
			)
				.map(el => el.textContent)
				.filter(text => text && (text.includes('guid') || text.includes('typeName') || text.includes('attributes')));

			const sortedNames = [...propertyNames].sort();
			expect(propertyNames).toEqual(sortedNames);
		});
	});

	describe('getValues Integration', () => {
		test('calls getValues with correct parameters for technical properties', () => {
			const entity = {
				guid: 'test-guid',
				label: 'test-label'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText('test-guid')).toBeInTheDocument();
			expect(screen.getByText('test-label')).toBeInTheDocument();
		});

		test('calls getValues with correct parameters for End1 properties', () => {
			const entity = {
				guid: 'test-guid',
				end1: {
					guid: 'end1-guid',
					typeName: 'End1Type'
				}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText('end1-guid')).toBeInTheDocument();
			expect(screen.getByText('End1Type')).toBeInTheDocument();
		});

		test('calls getValues with correct parameters for End2 properties', () => {
			const entity = {
				guid: 'test-guid',
				end2: {
					guid: 'end2-guid',
					typeName: 'End2Type'
				}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText('end2-guid')).toBeInTheDocument();
			expect(screen.getByText('End2Type')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		test('handles null entity', () => {
			render(<RelationshipPropertiesTab entity={null} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[0]).toHaveTextContent('No Record Found');
		});

		test('handles undefined entity', () => {
			render(<RelationshipPropertiesTab entity={undefined} loading={false} />);

			const accordionDetails = screen.getAllByTestId('accordion-details');
			expect(accordionDetails[0]).toHaveTextContent('No Record Found');
		});

		test('handles entity with extra properties not in pick list', () => {
			const entity = {
				guid: 'test-guid',
				extraProp1: 'value1',
				extraProp2: 'value2',
				label: 'test-label'
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.getAllByText(/label/).length).toBeGreaterThan(0);
			expect(screen.queryByText(/extraProp1/)).not.toBeInTheDocument();
			expect(screen.queryByText(/extraProp2/)).not.toBeInTheDocument();
		});

		test('handles complex nested objects in properties', () => {
			const entity = {
				guid: 'test-guid',
				propagatedClassifications: [
					{ typeName: 'class1', attributes: {} },
					{ typeName: 'class2', attributes: {} }
				]
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getByText(/propagatedClassifications \(2\)/)).toBeInTheDocument();
		});

		test('handles empty string values', () => {
			const entity = {
				guid: 'test-guid',
				label: '',
				status: ''
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.queryByText(/label/)).not.toBeInTheDocument();
			expect(screen.queryByText(/status/)).not.toBeInTheDocument();
		});

		test('handles zero values', () => {
			const entity = {
				guid: 'test-guid',
				version: 0
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.getByText(/version/)).toBeInTheDocument();
		});

		test('handles boolean values', () => {
			const entity = {
				guid: 'test-guid',
				propagateTags: false
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.getByText(/propagateTags/)).toBeInTheDocument();
		});

		test('handles empty arrays', () => {
			const entity = {
				guid: 'test-guid',
				blockedPropagatedClassifications: []
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.queryByText(/blockedPropagatedClassifications/)).not.toBeInTheDocument();
		});
	});

	describe('Accordion Default States', () => {
		test('Technical Properties accordion is expanded by default', () => {
			const entity = { guid: 'test-guid' };

			const { container } = render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordions = container.querySelectorAll('[data-testid="accordion"]');
			expect(accordions[0]).toHaveAttribute('data-default-expanded', 'true');
		});

		test('Relationship Properties accordion is expanded by default', () => {
			const entity = { guid: 'test-guid' };

			const { container } = render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordions = container.querySelectorAll('[data-testid="accordion"]');
			expect(accordions[1]).toHaveAttribute('data-default-expanded', 'true');
		});

		test('End1 accordion is collapsed by default', () => {
			const entity = { guid: 'test-guid' };

			const { container } = render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordions = container.querySelectorAll('[data-testid="accordion"]');
			expect(accordions[2]).toHaveAttribute('data-default-expanded', 'false');
		});

		test('End2 accordion is collapsed by default', () => {
			const entity = { guid: 'test-guid' };

			const { container } = render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			const accordions = container.querySelectorAll('[data-testid="accordion"]');
			expect(accordions[3]).toHaveAttribute('data-default-expanded', 'false');
		});
	});

	describe('Complete Entity with All Properties', () => {
		test('renders complete entity with all technical, end1, and end2 properties', () => {
			const entity = {
				createTime: 1640995200000,
				createdBy: 'test-user',
				blockedPropagatedClassifications: ['class1'],
				guid: 'test-guid-123',
				label: 'test-label',
				propagateTags: 'BOTH',
				propagatedClassifications: ['prop1'],
				provenanceType: 'NATIVE',
				status: 'ACTIVE',
				updateTime: 1640995300000,
				updatedBy: 'update-user',
				version: 1,
				end1: {
					guid: 'end1-guid',
					typeName: 'End1Type',
					uniqueAttributes: { qualifiedName: 'end1@cluster' }
				},
				end2: {
					guid: 'end2-guid',
					typeName: 'End2Type',
					uniqueAttributes: { qualifiedName: 'end2@cluster' }
				}
			};

			render(<RelationshipPropertiesTab entity={entity} loading={false} />);

			// Check all sections are rendered
			expect(screen.getByText('Technical Properties')).toBeInTheDocument();
			expect(screen.getByText('Relationship Properties')).toBeInTheDocument();
			expect(screen.getByText('End1')).toBeInTheDocument();
			expect(screen.getByText('End2')).toBeInTheDocument();

			// Check some properties are rendered
			expect(screen.getAllByText(/guid/).length).toBeGreaterThan(0);
			expect(screen.getByText(/version/)).toBeInTheDocument();
		});
	});
});
