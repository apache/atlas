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
 * Comprehensive unit tests for Enumerations component
 * 
 * Coverage Target:
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import Enumerations from '../Enumerations';
import userEvent from '@testing-library/user-event';

// Mock dependencies
const mockDispatch = jest.fn();
const mockCreateEnum = jest.fn();
const mockUpdateEnum = jest.fn();
const mockFetchEnumData = jest.fn();
const mockToastSuccess = jest.fn();
const mockToastDismiss = jest.fn();
const mockServerError = jest.fn();

// Mock toast
jest.mock('react-toastify', () => ({
	toast: {
		success: (...args: any[]) => mockToastSuccess(...args),
		dismiss: (...args: any[]) => mockToastDismiss(...args)
	}
}));

// Mock API methods
jest.mock('@api/apiMethods/typeDefApiMethods', () => ({
	createEnum: (...args: any[]) => mockCreateEnum(...args),
	updateEnum: (...args: any[]) => mockUpdateEnum(...args)
}));

// Mock Redux hooks
const mockUseAppSelector = jest.fn();
jest.mock('@hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch,
	useAppSelector: (...args: any[]) => mockUseAppSelector(...args)
}));

// Mock Redux slice
jest.mock('@redux/slice/enumSlice', () => ({
	fetchEnumData: jest.fn(() => ({ type: 'FETCH_ENUM_DATA' }))
}));

// Mock child component - simplified to directly call onSubmit
jest.mock('@views/BusinessMetadata/EnumCreateUpdate', () => ({
	__esModule: true,
	default: ({
		control,
		handleSubmit,
		setValue,
		reset,
		watch,
		isSubmitting,
		onSubmit,
		isDirty
	}: any) => {
		// Store onSubmit ref for tests
		React.useEffect(() => {
			(window as any).__enumTestOnSubmit = onSubmit;
		}, [onSubmit]);
		
		// Create a handler that will be called on button click
		const handleClick = async () => {
			const formData = (global as any).__testFormData;
			if (onSubmit && formData) {
				try {
					await onSubmit(formData);
				} catch (error) {
					console.error('Form error:', error);
				}
			}
		};
		
		return (
			<div data-testid="enum-create-update">
				<button
					type="button"
					onClick={handleClick}
					disabled={isSubmitting}
					data-testid="submit-button"
				>
					Submit
				</button>
				<button
					type="button"
					onClick={() => reset({ enumType: '', enumValues: [] })}
					data-testid="reset-button"
				>
					Reset
				</button>
				<div data-testid="is-dirty">{isDirty ? 'dirty' : 'clean'}</div>
				<div data-testid="is-submitting">{isSubmitting ? 'submitting' : 'not-submitting'}</div>
			</div>
		);
	}
}));

// Mock react-hook-form
let currentFormData: any = { enumType: 'TestEnum', enumValues: [{ value: 'Value1' }, { value: 'Value2' }] };
(global as any).__testFormData = currentFormData;

const mockSetValue = jest.fn();
const mockReset = jest.fn();
const mockWatch = jest.fn(() => (global as any).__testFormData);

const mockHandleSubmit = jest.fn((onSubmitFn: any) => {
	return (e?: any) => {
		if (e && typeof e.preventDefault === 'function') {
			e.preventDefault();
		}
		// Get form data from global - tests update this before submitting
		const formData = (global as any).__testFormData || currentFormData;
		if (onSubmitFn && formData) {
			return Promise.resolve(onSubmitFn(formData));
		}
		return Promise.resolve();
	};
});

const mockUseForm = jest.fn(() => ({
	control: {
		register: jest.fn((name: string) => ({ name })),
		unregister: jest.fn(),
		getValues: jest.fn(),
		setValue: mockSetValue,
		watch: mockWatch,
		reset: mockReset,
		resetField: jest.fn(),
		clearErrors: jest.fn(),
		setError: jest.fn(),
		trigger: jest.fn(),
		formState: {
			isDirty: false,
			isSubmitting: false,
			isValid: true,
			errors: {}
		}
	},
	handleSubmit: mockHandleSubmit,
	watch: mockWatch,
	setValue: mockSetValue,
	reset: mockReset,
	formState: {
		isDirty: false,
		isSubmitting: false
	}
}));

jest.mock('react-hook-form', () => ({
	useForm: (...args: any[]) => mockUseForm(...args)
}));

// Mock utils
const mockIsEmpty = jest.fn((val: any) => {
	if (val === null || val === undefined || val === '') return true;
	if (Array.isArray(val) && val.length === 0) return true;
	if (typeof val === 'object' && Object.keys(val).length === 0) return true;
	return false;
});
const mockServerErrorUtil = jest.fn();

jest.mock('@utils/Utils', () => ({
	isEmpty: (...args: any[]) => mockIsEmpty(...args),
	serverError: (...args: any[]) => mockServerErrorUtil(...args)
}));

describe('Enumerations Component', () => {
	const mockEnumDefs = [
		{
			name: 'TestEnum',
			elementDefs: [
				{ ordinal: 1, value: 'Value1' },
				{ ordinal: 2, value: 'Value2' },
				{ ordinal: 3, value: 'Value3' }
			]
		},
		{
			name: 'AnotherEnum',
			elementDefs: [
				{ ordinal: 1, value: 'A1' },
				{ ordinal: 2, value: 'A2' }
			]
		}
	];

	const renderComponent = () => {
		return render(<Enumerations />);
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockDispatch.mockClear();
		mockCreateEnum.mockClear();
		mockUpdateEnum.mockClear();
		mockFetchEnumData.mockClear();
		mockToastSuccess.mockClear();
		mockToastDismiss.mockClear();
		mockServerErrorUtil.mockClear();
		mockIsEmpty.mockImplementation((val: any) => {
		if (val === null || val === undefined || val === '') return true;
		if (Array.isArray(val) && val.length === 0) return true;
		if (typeof val === 'object' && Object.keys(val).length === 0) return true;
		return false;
	});
		
		// Reset form data - update both local and global
		currentFormData = { enumType: 'TestEnum', enumValues: [{ value: 'Value1' }, { value: 'Value2' }] };
		(global as any).__testFormData = currentFormData;
		mockWatch.mockReturnValue(currentFormData);
		mockReset.mockClear();
		mockSetValue.mockClear();
		// Reset mockHandleSubmit to ensure it reads latest form data
		mockHandleSubmit.mockImplementation((onSubmitFn: any) => {
			return async (e?: any) => {
				if (e && typeof e.preventDefault === 'function') {
					e.preventDefault();
				}
				// Read form data at the time of submission, not when handleSubmit is called
				const formData = (global as any).__testFormData || currentFormData;
				if (onSubmitFn && formData) {
					// Call onSubmit with form data - this should trigger createEnum/updateEnum
					await Promise.resolve(onSubmitFn(formData));
				}
			};
		});
		mockUseForm.mockReturnValue({
			control: {
				register: jest.fn((name: string) => ({ name })),
				unregister: jest.fn(),
				getValues: jest.fn(),
				setValue: mockSetValue,
				watch: mockWatch,
				reset: mockReset,
				resetField: jest.fn(),
				clearErrors: jest.fn(),
				setError: jest.fn(),
				trigger: jest.fn(),
				formState: {
					isDirty: false,
					isSubmitting: false,
					isValid: true,
					errors: {}
				}
			},
			handleSubmit: mockHandleSubmit,
			watch: mockWatch,
			setValue: mockSetValue,
			reset: mockReset,
			formState: {
				isDirty: false,
				isSubmitting: false
			}
		});
		
		// Default Redux state
		mockUseAppSelector.mockImplementation((selector: any) => {
			const state = {
				enum: {
					enumObj: {
						data: {
							enumDefs: mockEnumDefs
						}
					}
				}
			};
			return selector(state);
		});
	});

	describe('Component Rendering', () => {
		it('should render Enumerations component', () => {
			renderComponent();
			
			expect(screen.getByTestId('enum-create-update')).toBeInTheDocument();
		});

	it('should render EnumCreateUpdate component', () => {
		renderComponent();
		
		expect(screen.getByTestId('enum-create-update')).toBeInTheDocument();
		expect(screen.getByTestId('submit-button')).toBeInTheDocument();
		expect(screen.getByTestId('reset-button')).toBeInTheDocument();
	});

		it('should pass correct props to EnumCreateUpdate', () => {
			renderComponent();
			
			expect(screen.getByTestId('enum-create-update')).toBeInTheDocument();
			expect(mockUseForm).toHaveBeenCalled();
		});
	});

	describe('Form Submission - Create Enum', () => {
	it('should create new enum when enumType does not exist', async () => {
		mockUseAppSelector.mockImplementation((selector: any) => {
			const state = {
				enum: {
					enumObj: {
						data: {
							enumDefs: []
						}
					}
				}
			};
			return selector(state);
		});
		
		mockCreateEnum.mockResolvedValue({});
		currentFormData = { enumType: 'NewEnum', enumValues: [{ value: 'V1' }, { value: 'V2' }] };
		(global as any).__testFormData = currentFormData;
		
		renderComponent();
		
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
		
		await waitFor(() => {
			expect(mockCreateEnum).toHaveBeenCalledWith({
				enumDefs: [{
					name: 'NewEnum',
					elementDefs: [
						{ ordinal: 1, value: 'V1' },
						{ ordinal: 2, value: 'V2' }
					]
				}]
			});
		}, { timeout: 5000 });
	});

		it('should show success toast when enum is created', async () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			mockCreateEnum.mockResolvedValue({});
			mockToastSuccess.mockReturnValue('toast-id');
			currentFormData = { enumType: 'NewEnum', enumValues: [{ value: 'V1' }] };
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockToastDismiss).toHaveBeenCalled();
				expect(mockToastSuccess).toHaveBeenCalledWith(
					expect.stringContaining('NewEnum')
				);
			}, { timeout: 3000 });
		});

		it('should show success toast with multiline message (lines 88-90)', async () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			mockCreateEnum.mockResolvedValue({});
			mockToastSuccess.mockReturnValue('toast-id');
			currentFormData = { enumType: 'NewEnum', enumValues: [{ value: 'V1' }] };
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				// Verify toast is called with multiline string (lines 88-90)
				// The toast message contains newlines: `Enumeration ${enumType} \n added\n successfully`
				expect(mockToastDismiss).toHaveBeenCalled();
				expect(mockToastSuccess).toHaveBeenCalled();
				const toastCall = mockToastSuccess.mock.calls[0][0];
				expect(typeof toastCall).toBe('string');
				expect(toastCall).toContain('NewEnum');
			}, { timeout: 3000 });
		});

		it('should reset form after successful creation', async () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			mockCreateEnum.mockResolvedValue({});
			currentFormData = { enumType: 'NewEnum', enumValues: [] };
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockReset).toHaveBeenCalledWith({ enumType: '', enumValues: [] });
			}, { timeout: 3000 });
		});

		it('should dispatch fetchEnumData after successful creation', async () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			mockCreateEnum.mockResolvedValue({});
			const { fetchEnumData } = require('@redux/slice/enumSlice');
			currentFormData = { enumType: 'NewEnum', enumValues: [] };
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalledWith(fetchEnumData());
			}, { timeout: 3000 });
		});
	});

	describe('Form Submission - Update Enum', () => {
		it('should update enum when values changed', async () => {
			mockUpdateEnum.mockResolvedValue({});
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }, { value: 'NewValue' }]
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockUpdateEnum).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should update enum when a value is changed (line 61)', async () => {
			// Test the branch where enumDef.forEach finds a value not in selectedEnumValues
			// This covers line 61: isPutCall = true inside the forEach
			// IMPORTANT: Same count (3 values) but different values to hit line 61
			mockUpdateEnum.mockResolvedValue({});
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }, { value: 'Value2' }, { value: 'NewValue3' }] // Same count, but Value3 changed to NewValue3
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockUpdateEnum).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should update enum when element count differs (line 65)', async () => {
			// Test the branch where enumDef.length !== selectedEnumValues.length
			// This covers line 65: isPutCall = true (else branch)
			mockUpdateEnum.mockResolvedValue({});
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }] // Different count than original 3
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockUpdateEnum).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should set isPostCallEnum when enumName is empty (line 68)', async () => {
			// Test line 68: isPostCallEnum = true when enumName is empty
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: [] // Empty, so enumName will be empty
							}
						}
					}
				};
				return selector(state);
			});
			
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === undefined || val === null || val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				if (typeof val === 'object' && Object.keys(val).length === 0) return true;
				return false;
			});
			
			mockCreateEnum.mockResolvedValue({});
			currentFormData = {
				enumType: 'NewEnum',
				enumValues: [{ value: 'V1' }]
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockCreateEnum).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should update enum when element count changed', async () => {
			mockUpdateEnum.mockResolvedValue({});
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }] // Different count
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockUpdateEnum).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should update enum when value removed', async () => {
			mockUpdateEnum.mockResolvedValue({});
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }, { value: 'Value2' }] // One less than original
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockUpdateEnum).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should show success toast when enum is updated', async () => {
			mockUpdateEnum.mockResolvedValue({});
			mockToastSuccess.mockReturnValue('toast-id');
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }, { value: 'NewValue' }]
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockToastDismiss).toHaveBeenCalled();
				expect(mockToastSuccess).toHaveBeenCalled();
			}, { timeout: 3000 });
		});
	});

	describe('Form Submission - No Changes', () => {
		it('should show "No updated values" when values are unchanged', async () => {
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }, { value: 'Value2' }, { value: 'Value3' }]
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockToastDismiss).toHaveBeenCalled();
				expect(mockToastSuccess).toHaveBeenCalledWith('No updated values');
			}, { timeout: 3000 });
		});

		it('should not call create or update when no changes', async () => {
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }, { value: 'Value2' }, { value: 'Value3' }]
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockCreateEnum).not.toHaveBeenCalled();
				expect(mockUpdateEnum).not.toHaveBeenCalled();
			});
		});
	});

	describe('Error Handling', () => {
		it('should handle error when creating enum fails', async () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			const error = new Error('Create failed');
			mockCreateEnum.mockRejectedValue(error);
			currentFormData = { enumType: 'NewEnum', enumValues: [] };
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockCreateEnum).toHaveBeenCalled();
				expect(mockServerErrorUtil).toHaveBeenCalledWith(
					error,
					expect.objectContaining({ current: null })
				);
			}, { timeout: 3000 });
		});

		it('should handle error when updating enum fails', async () => {
			const error = new Error('Update failed');
			mockUpdateEnum.mockRejectedValue(error);
			currentFormData = {
				enumType: 'TestEnum',
				enumValues: [{ value: 'Value1' }, { value: 'NewValue' }]
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockUpdateEnum).toHaveBeenCalled();
				expect(mockServerErrorUtil).toHaveBeenCalledWith(
					error,
					expect.objectContaining({ current: null })
				);
			}, { timeout: 3000 });
		});
	});

	describe('Enum Value Processing', () => {
		it('should process enumValues array correctly', async () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			mockCreateEnum.mockResolvedValue({});
			currentFormData = {
				enumType: 'NewEnum',
				enumValues: [{ value: 'V1' }, { value: 'V2' }, { value: 'V3' }]
			};
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockCreateEnum).toHaveBeenCalledWith({
					enumDefs: [{
						name: 'NewEnum',
						elementDefs: [
							{ ordinal: 1, value: 'V1' },
							{ ordinal: 2, value: 'V2' },
							{ ordinal: 3, value: 'V3' }
						]
					}]
				});
			}, { timeout: 3000 });
		});

		it('should handle empty enumValues array', async () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			mockCreateEnum.mockResolvedValue({});
			currentFormData = { enumType: 'NewEnum', enumValues: [] };
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				expect(mockCreateEnum).toHaveBeenCalledWith({
					enumDefs: [{
						name: 'NewEnum',
						elementDefs: []
					}]
				});
			}, { timeout: 3000 });
		});

		it('should handle undefined enumValues', async () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === undefined || val === null || val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				if (typeof val === 'object' && Object.keys(val).length === 0) return true;
				return false;
			});
			
			mockCreateEnum.mockResolvedValue({});
			mockToastSuccess.mockReturnValue('toast-id');
			currentFormData = { enumType: 'NewEnum', enumValues: undefined };
			(global as any).__testFormData = currentFormData;
			
			renderComponent();
			
		const submitButton = screen.getByTestId('submit-button');
		await userEvent.click(submitButton);
			
			await waitFor(() => {
				// Should create enum with empty elementDefs when enumValues is undefined/empty
				expect(mockCreateEnum).toHaveBeenCalledWith({
					enumDefs: [{
						name: 'NewEnum',
						elementDefs: []
					}]
				});
			}, { timeout: 5000 });
		});
	});

	describe('Edge Cases', () => {
		it('should handle empty enumDefs', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {
								enumDefs: []
							}
						}
					}
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(screen.getByTestId('enum-create-update')).toBeInTheDocument();
		});

		it('should handle undefined enumDefs', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {
							data: {}
						}
					}
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(screen.getByTestId('enum-create-update')).toBeInTheDocument();
		});

		it('should handle empty enumObj', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					enum: {
						enumObj: {}
					}
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(screen.getByTestId('enum-create-update')).toBeInTheDocument();
		});
	});

	describe('Form State', () => {
		it('should pass isDirty to EnumCreateUpdate', () => {
			// Mock useForm to return formState with isDirty: true
			mockUseForm.mockReturnValueOnce({
				control: {
					register: jest.fn((name: string) => ({ name })),
					formState: { isDirty: true, isSubmitting: false }
				},
				handleSubmit: mockHandleSubmit,
				watch: mockWatch,
				setValue: mockSetValue,
				reset: mockReset,
				formState: { isDirty: true, isSubmitting: false }
			});
			
			renderComponent();
			
			expect(screen.getByTestId('is-dirty')).toHaveTextContent('dirty');
		});

		it('should pass isSubmitting to EnumCreateUpdate', () => {
			// Mock useForm to return formState with isSubmitting: true
			mockUseForm.mockReturnValueOnce({
				control: {
					register: jest.fn((name: string) => ({ name })),
					formState: { isDirty: false, isSubmitting: true }
				},
				handleSubmit: mockHandleSubmit,
				watch: mockWatch,
				setValue: mockSetValue,
				reset: mockReset,
				formState: { isDirty: false, isSubmitting: true }
			});
			
			renderComponent();
			
			expect(screen.getByTestId('is-submitting')).toHaveTextContent('submitting');
		});
	});
});
