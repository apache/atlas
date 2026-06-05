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
 * Comprehensive unit tests for SystemDetails component
 * 
 * Coverage Target: 100% (Statements, Branches, Functions, Lines)
 */

import React from 'react'
import { render, screen } from '@utils/test-utils'
import SystemDetails from '../SystemDetails'

jest.mock('../../../hooks/reducerHook', () => ({
	useAppSelector: jest.fn((selector: any) =>
		selector({
			metrics: {
				metricsData: {
					data: {
						system: {
							os: {
								name: 'Linux',
								version: '5.4.0',
								arch: 'x64'
							},
							runtime: {
								name: 'Node.js',
								version: 'v16.0.0',
								pid: 12345
							},
							memory: {
								total: 8589934592,
								free: 4294967296,
								used: 4294967296,
								nested: {
									heap: {
										total: 1073741824,
										used: 536870912
									}
								}
							}
						}
					}
				}
			}
		})
	)
}))

jest.mock('@components/muiComponents', () => ({
	Accordion: ({ children, defaultExpanded }: any) => (
		<div data-testid="accordion" data-expanded={defaultExpanded}>{children}</div>
	),
	AccordionSummary: ({ children }: any) => <div data-testid="accordion-summary">{children}</div>,
	AccordionDetails: ({ children }: any) => <div data-testid="accordion-details">{children}</div>
}))

const mockNumberFormatWithBytes = jest.fn((val: number) => `${val} bytes`)
const mockCustomSortObj = jest.fn((obj: any) => obj)
const mockGetValues = jest.fn((obj: any) => JSON.stringify(obj))

jest.mock('@utils/Helper', () => ({
	customSortObj: (...args: any[]) => mockCustomSortObj(...args),
	numberFormatWithBytes: (...args: any[]) => mockNumberFormatWithBytes(...args)
}))

jest.mock('@components/commonComponents', () => ({
	getValues: (...args: any[]) => mockGetValues(...args)
}))

const mockIsObject = jest.fn((val: any) => typeof val === 'object' && val !== null && !Array.isArray(val))

jest.mock('@utils/Utils', () => ({
	isObject: (...args: any[]) => mockIsObject(...args)
}))

describe('SystemDetails', () => {
	const { useAppSelector } = require('../../../hooks/reducerHook')

	beforeEach(() => {
		jest.clearAllMocks()
		mockNumberFormatWithBytes.mockImplementation((val: number) => `${val} bytes`)
		mockCustomSortObj.mockImplementation((obj: any) => obj)
		mockGetValues.mockImplementation((obj: any) => JSON.stringify(obj))
		mockIsObject.mockImplementation((val: any) => typeof val === 'object' && val !== null && !Array.isArray(val))
		useAppSelector.mockImplementation((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							system: {
								os: {
									name: 'Linux',
									version: '5.4.0',
									arch: 'x64'
								},
								runtime: {
									name: 'Node.js',
									version: 'v16.0.0',
									pid: 12345
								},
								memory: {
									total: 8589934592,
									free: 4294967296,
									used: 4294967296,
									nested: {
										heap: {
											total: 1073741824,
											used: 536870912
										}
									}
								}
							}
						}
					}
				}
			})
		)
	})

	it('renders system details with OS, Runtime, and Memory data', () => {
		render(<SystemDetails />)

		expect(screen.getByText('System Details')).toBeInTheDocument()
		expect(screen.getByText('OS')).toBeInTheDocument()
		expect(screen.getByText('Runtime')).toBeInTheDocument()
		expect(screen.getByText('Memory')).toBeInTheDocument()
		// "name" appears in both OS and Runtime sections, so use getAllByText
		const nameElements = screen.getAllByText('name')
		expect(nameElements.length).toBeGreaterThan(0)
		expect(screen.getByText('Linux')).toBeInTheDocument()
		expect(screen.getByText('Node.js')).toBeInTheDocument()
	})

	it('renders OS data correctly', () => {
		render(<SystemDetails />)

		// "version" appears in both OS and Runtime sections, so use getAllByText
		const versionElements = screen.getAllByText('version')
		expect(versionElements.length).toBeGreaterThan(0)
		expect(screen.getByText('5.4.0')).toBeInTheDocument()
		expect(screen.getByText('arch')).toBeInTheDocument()
		expect(screen.getByText('x64')).toBeInTheDocument()
	})

	it('renders Runtime data correctly', () => {
		render(<SystemDetails />)

		expect(screen.getByText('pid')).toBeInTheDocument()
		expect(screen.getByText('12345')).toBeInTheDocument()
	})

	it('renders Memory data with numberFormatWithBytes for non-object values', () => {
		render(<SystemDetails />)

		expect(mockNumberFormatWithBytes).toHaveBeenCalledWith(8589934592)
		expect(mockNumberFormatWithBytes).toHaveBeenCalledWith(4294967296)
	})

	it('renders Memory data with getValues for object values', () => {
		render(<SystemDetails />)

		expect(mockGetValues).toHaveBeenCalledWith(
			{ heap: { total: 1073741824, used: 536870912 } },
			undefined,
			undefined,
			undefined,
			'properties'
		)
	})

	it('handles empty system data', () => {
		useAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							system: {
								os: {},
								runtime: {},
								memory: {}
							}
						}
					}
				}
			})
		)

		render(<SystemDetails />)

		expect(screen.getByText('System Details')).toBeInTheDocument()
	})

	it('handles undefined system data', () => {
		useAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {}
					}
				}
			})
		)

		render(<SystemDetails />)

		expect(screen.getByText('System Details')).toBeInTheDocument()
	})

	it('handles null system data', () => {
		useAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							system: null
						}
					}
				}
			})
		)

		render(<SystemDetails />)

		expect(screen.getByText('System Details')).toBeInTheDocument()
	})

	it('handles memory data with mixed object and non-object values', () => {
		useAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							system: {
								memory: {
									total: 1000,
									nested: { key: 'value' }
								}
							}
						}
					}
				}
			})
		)

		mockIsObject.mockImplementation((val: any) => {
			if (val === 1000) return false
			if (typeof val === 'object' && val !== null) return true
			return false
		})

		render(<SystemDetails />)

		expect(mockNumberFormatWithBytes).toHaveBeenCalledWith(1000)
		expect(mockGetValues).toHaveBeenCalled()
	})
})
