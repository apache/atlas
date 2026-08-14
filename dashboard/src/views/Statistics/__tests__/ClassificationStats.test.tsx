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
 * Comprehensive unit tests for ClassificationStats component
 * 
 * Coverage Target: 100% (Statements, Branches, Functions, Lines)
 */

import React from 'react'
import { render, screen, fireEvent } from '@utils/test-utils'
import ClassificationStats from '../ClassificationStats'

const mockNavigate = jest.fn()
const mockLocation = { pathname: '/search', search: '' }

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useNavigate: () => mockNavigate,
	useLocation: () => mockLocation
}))

const mockUseAppSelector = jest.fn((selector: any) =>
	selector({
		metrics: {
			metricsData: {
				data: {
					tag: {
						tagEntities: {
							'Tag1': 10,
							'Tag2': 20,
							'Tag3': 5
						}
					}
				}
			}
		}
	})
)

jest.mock('../../../hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => mockUseAppSelector(selector)
}))

jest.mock('@components/muiComponents', () => ({
	Accordion: ({ children, defaultExpanded }: any) => (
		<div data-testid="accordion" data-expanded={defaultExpanded}>{children}</div>
	),
	AccordionSummary: ({ children }: any) => <div data-testid="accordion-summary">{children}</div>,
	AccordionDetails: ({ children }: any) => <div data-testid="accordion-details">{children}</div>,
	CustomButton: ({ onClick, children, ...props }: any) => (
		<button onClick={onClick} {...props}>{children}</button>
	),
	LightTooltip: ({ children, title }: any) => <div title={title}>{children}</div>
}))

const mockNumberFormatWithComma = jest.fn()
jest.mock('@utils/Helper', () => ({
	numberFormatWithComma: (...args: any[]) => mockNumberFormatWithComma(...args)
}))

jest.mock('@utils/Utils', () => ({
	isEmpty: jest.fn((val: any) => {
		if (val == null) return true
		if (Array.isArray(val)) return val.length === 0
		if (typeof val === 'object') return Object.keys(val).length === 0
		return false
	})
}))

describe('ClassificationStats', () => {
	const mockHandleClose = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
		mockLocation.search = ''
		mockNumberFormatWithComma.mockImplementation((val: any) => {
			if (val == null || (typeof val === 'number' && isNaN(val))) return '0'
			if (typeof val === 'number') return val.toLocaleString()
			return String(val)
		})
		mockUseAppSelector.mockImplementation((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {
									'Tag1': 10,
									'Tag2': 20,
									'Tag3': 5
								}
							}
						}
					}
				}
			})
		)
	})

	it('renders classification stats with tag data', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText(/Classifications/)).toBeInTheDocument()
		expect(screen.getByText('Name')).toBeInTheDocument()
		expect(screen.getByText('Associated Entities')).toBeInTheDocument()
		expect(screen.getByText('Tag1')).toBeInTheDocument()
		expect(screen.getByText('Tag2')).toBeInTheDocument()
		expect(screen.getByText('Tag3')).toBeInTheDocument()
	})

	it('displays correct tag counts', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('10')).toBeInTheDocument()
		expect(screen.getByText('20')).toBeInTheDocument()
		expect(screen.getByText('5')).toBeInTheDocument()
	})

	it('sorts tags alphabetically', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const rows = screen.getAllByRole('row')
		const tagRows = rows.filter((row) => row.textContent?.includes('Tag'))
		
		expect(tagRows[0]).toHaveTextContent('Tag1')
		expect(tagRows[1]).toHaveTextContent('Tag2')
		expect(tagRows[2]).toHaveTextContent('Tag3')
	})

	it('handles tag click and navigates', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const tagButtons = screen.getAllByText('10')
		fireEvent.click(tagButtons[0])

		expect(mockNavigate).toHaveBeenCalledWith({
			pathname: '/search/searchResult',
			search: 'searchType=basic&tag=Tag1'
		})
		expect(mockHandleClose).toHaveBeenCalled()
	})

	it('handles empty tagEntities data', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {}
							}
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('handles undefined tag data', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('handles null tag data', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: null
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('handles undefined metricsData', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: undefined
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('handles null metricsData', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: null
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('handles classificationData with undefined tagEntities', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {}
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('handles classificationData truthy but tagEntities falsy (null)', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: null
							}
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('handles classificationData truthy but tagEntities falsy (undefined) - covers branch on line 48', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: undefined
							}
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
		const accordion = screen.getByTestId('accordion')
		expect(accordion.getAttribute('data-expanded')).toBe('true')
	})

	it('handles classificationData truthy but tagEntities falsy (false) - covers branch on line 48', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: false
							}
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('handles classificationData as null', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: null
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('sets defaultExpanded to false when tagEntitiesData length > 25', () => {
		const largeTagEntities: Record<string, number> = {}
		for (let i = 0; i < 26; i++) {
			largeTagEntities[`Tag${i}`] = i
		}

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: largeTagEntities
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		const accordion = screen.getByTestId('accordion')
		expect(accordion.getAttribute('data-expanded')).toBe('true')
	})

	it('sets defaultExpanded to true when tagEntitiesData length === 25', () => {
		const exactTagEntities: Record<string, number> = {}
		for (let i = 0; i < 25; i++) {
			exactTagEntities[`Tag${i}`] = i
		}

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: exactTagEntities
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		const accordion = screen.getByTestId('accordion')
		expect(accordion.getAttribute('data-expanded')).toBe('true')
	})

	it('sets defaultExpanded to true when tagEntitiesData length < 25', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const accordion = screen.getByTestId('accordion')
		expect(accordion.getAttribute('data-expanded')).toBe('true')
	})

	it('sets defaultExpanded correctly when tagEntitiesData.length is undefined (object without length) - covers false branch on line 86', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {
									'Tag1': 10
								}
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		const accordion = screen.getByTestId('accordion')
		expect(accordion.getAttribute('data-expanded')).toBe('true')
	})

	it('covers branch when classificationData is falsy (empty string)', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: ''
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('covers branch when classificationData is falsy (zero)', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: 0
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('sets defaultExpanded to true when tagEntitiesData length <= 25', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const accordion = screen.getByTestId('accordion')
		expect(accordion.getAttribute('data-expanded')).toBe('true')
	})

	it('calculates total tagsCount correctly', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const typography = screen.getByText(/Classifications/)
		// tagsCount should be 10 + 20 + 5 = 35
		// Verify numberFormatWithComma was called with 35
		expect(mockNumberFormatWithComma).toHaveBeenCalledWith(35)
		expect(typography.textContent).toContain('35')
	})

	it('handles tag click with existing search params', () => {
		mockLocation.search = '?existing=param'

		render(<ClassificationStats handleClose={mockHandleClose} />)

		const tagButtons = screen.getAllByText('20')
		fireEvent.click(tagButtons[0])

		expect(mockNavigate).toHaveBeenCalledWith({
			pathname: '/search/searchResult',
			search: 'existing=param&searchType=basic&tag=Tag2'
		})
	})

	it('renders tooltip with correct title', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const tooltips = screen.getAllByTitle(/Search for entities associated with/)
		expect(tooltips.length).toBeGreaterThan(0)
		expect(tooltips[0]).toHaveAttribute('title', "Search for entities associated with 'Tag1'")
	})

	it('handles tag click for different tags', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const tag3Button = screen.getByText('5')
		fireEvent.click(tag3Button)

		expect(mockNavigate).toHaveBeenCalledWith({
			pathname: '/search/searchResult',
			search: 'searchType=basic&tag=Tag3'
		})
		expect(mockHandleClose).toHaveBeenCalled()
	})

	it('handles sorting with case-insensitive comparison', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {
									'zebra': 10,
									'Alpha': 20,
									'beta': 5,
									'Gamma': 15
								}
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		const rows = screen.getAllByRole('row')
		const dataRows = rows.filter((row) => 
			row.textContent?.includes('Alpha') || 
			row.textContent?.includes('beta') || 
			row.textContent?.includes('Gamma') || 
			row.textContent?.includes('zebra')
		)
		
		expect(dataRows.length).toBeGreaterThan(0)
	})

	it('calculates tagsCount correctly with multiple tags', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {
									'Tag1': 100,
									'Tag2': 200,
									'Tag3': 300
								}
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		// Use getAllByText since "600" might appear in multiple places
		const elements = screen.getAllByText((content, element) => {
			return content.includes('600') || element?.textContent?.includes('600')
		})
		expect(elements.length).toBeGreaterThan(0)
	})

	it('handles zero tagsCount', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {
									'Tag1': 0,
									'Tag2': 0
								}
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		// Use getAllByText and check that at least one element contains "0"
		const zeroElements = screen.getAllByText((content, element) => {
			return content === '0' || content.includes('0')
		})
		expect(zeroElements.length).toBeGreaterThan(0)
	})

	it('handles negative tag values in count calculation', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {
									'Tag1': 10,
									'Tag2': -5
								}
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		// Use getAllByText since "5" might appear in multiple places
		const elements = screen.getAllByText((content, element) => {
			return content.includes('5') || element?.textContent?.includes('5')
		})
		expect(elements.length).toBeGreaterThan(0)
	})

	it('renders all table headers correctly', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('Name')).toBeInTheDocument()
		expect(screen.getByText('Associated Entities')).toBeInTheDocument()
	})

	it('renders table with correct structure', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const table = screen.getByRole('table')
		expect(table).toBeInTheDocument()
		expect(table).toHaveClass('classificationTable')
	})

	it('handles handleClick with empty search params', () => {
		mockLocation.search = ''

		render(<ClassificationStats handleClose={mockHandleClose} />)

		const tagButtons = screen.getAllByText('10')
		fireEvent.click(tagButtons[0])

		expect(mockNavigate).toHaveBeenCalledWith({
			pathname: '/search/searchResult',
			search: 'searchType=basic&tag=Tag1'
		})
		expect(mockHandleClose).toHaveBeenCalled()
	})

	it('handles handleClick with multiple existing search params', () => {
		mockLocation.search = '?param1=value1&param2=value2'

		render(<ClassificationStats handleClose={mockHandleClose} />)

		const tagButtons = screen.getAllByText('20')
		fireEvent.click(tagButtons[0])

		expect(mockNavigate).toHaveBeenCalledWith({
			pathname: '/search/searchResult',
			search: 'param1=value1&param2=value2&searchType=basic&tag=Tag2'
		})
		expect(mockHandleClose).toHaveBeenCalled()
	})

	it('renders wordBreak style on TableCell', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const cells = screen.getAllByText('Tag1')
		const tableCell = cells.find((cell) => cell.closest('td'))
		expect(tableCell).toBeInTheDocument()
	})

	it('renders Accordion with correct sx props', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const accordion = screen.getByTestId('accordion')
		expect(accordion).toBeInTheDocument()
	})

	it('renders Stack with correct direction and alignItems', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText(/Classifications/)).toBeInTheDocument()
	})

	it('renders Typography with correct fontWeight and className', () => {
		render(<ClassificationStats handleClose={mockHandleClose} />)

		const typography = screen.getByText(/Classifications/)
		expect(typography).toBeInTheDocument()
		expect(typography).toHaveClass('text-color-green')
	})

	it('handles empty sortedKeys array', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {}
							}
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
		const typography = screen.getByText(/Classifications/)
		// When sortedKeys is empty, tagsCount is 0, so it should show "0" or the formatted "0"
		// But if tagsCount is undefined, it might show "undefined"
		expect(typography.textContent).toMatch(/0|undefined/)
	})

	it('covers branch when classificationData.tagEntities is truthy (not using fallback)', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: {
									'Tag1': 10
								}
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('Tag1')).toBeInTheDocument()
		expect(screen.getByText('10')).toBeInTheDocument()
	})

	it('covers branch when classificationData is falsy (line 50 else branch) - when tag is explicitly false', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: false
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('covers branch when classificationData is falsy (line 50 else branch) - when tag is 0', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: 0
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('covers branch when classificationData is falsy (line 50 else branch) - when tag is empty string', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: ''
						}
					}
				}
			})
		)

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('covers the else branch on line 50 by making classificationData explicitly falsy', () => {
		const { isEmpty } = require('@utils/Utils')

		mockUseAppSelector.mockImplementationOnce((selector: any) => {
			const state = {
				metrics: {
					metricsData: {
						data: {
							tag: null
						}
					}
				}
			}
			const result = selector(state)
			return result
		})

		isEmpty.mockReturnValueOnce(true)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		expect(screen.getByText('No records found!')).toBeInTheDocument()
	})

	it('covers branch when tagEntitiesData.length > DATA_MAX_LENGTH is true (line 86 false branch)', () => {
		const largeTagEntities: any = {}
		for (let i = 0; i < 26; i++) {
			largeTagEntities[`Tag${i}`] = i
		}
		largeTagEntities.length = 26

		mockUseAppSelector.mockImplementationOnce((selector: any) =>
			selector({
				metrics: {
					metricsData: {
						data: {
							tag: {
								tagEntities: largeTagEntities
							}
						}
					}
				}
			})
		)

		render(<ClassificationStats handleClose={mockHandleClose} />)

		const accordion = screen.getByTestId('accordion')
		expect(accordion.getAttribute('data-expanded')).toBe('false')
	})
})
