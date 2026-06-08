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
 * Comprehensive unit tests for DeleteGlossary component
 * 
 * Coverage Target: 100%
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import DeleteGlossary from '../DeleteGlossary';

// Mock functions
const mockNavigate = jest.fn();
const mockDispatch = jest.fn();
const mockOnClose = jest.fn();
const mockSetExpandNode = jest.fn();
const mockUpdatedData = jest.fn();
const mockDeleteGlossaryorTerm = jest.fn();
const mockDeleteGlossaryorType = jest.fn();
const mockFetchGlossaryData = jest.fn();
const mockIsEmpty = jest.fn();
const mockServerError = jest.fn();

// Mock location state
let mockLocation = {
	pathname: '/glossary',
	search: '',
	hash: '',
	state: null,
	key: 'test-key'
};

// Mock params state
let mockParams = { guid: undefined };

// Mock toast
const mockToastSuccess = jest.fn();
const mockToastDismiss = jest.fn();
const mockToastError = jest.fn();

// Mock react-router-dom
jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useNavigate: () => mockNavigate,
	useLocation: () => mockLocation,
	useParams: () => mockParams
}));

// Mock API methods
jest.mock('@api/apiMethods/glossaryApiMethod', () => ({
	deleteGlossaryorTerm: (...args: any[]) => mockDeleteGlossaryorTerm(...args),
	deleteGlossaryorType: (...args: any[]) => mockDeleteGlossaryorType(...args)
}));

// Mock Redux hooks
jest.mock('@hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch
}));

// Mock Redux slice
jest.mock('@redux/slice/glossarySlice', () => ({
	fetchGlossaryData: jest.fn(() => ({ type: 'glossary/fetchGlossaryData' }))
}));

// Mock utils
jest.mock('@utils/Utils', () => ({
	isEmpty: (...args: any[]) => mockIsEmpty(...args),
	serverError: (...args: any[]) => mockServerError(...args)
}));

// Mock toast
jest.mock('react-toastify', () => ({
	toast: {
		success: (...args: any[]) => mockToastSuccess(...args),
		dismiss: (...args: any[]) => mockToastDismiss(...args),
		error: (...args: any[]) => mockToastError(...args)
	}
}));

// Mock CustomModal
jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({ 
		open, 
		onClose, 
		title, 
		titleIcon, 
		button1Label, 
		button1Handler, 
		button2Label, 
		button2Handler, 
		children 
	}: any) =>
		open ? (
			<div data-testid="custom-modal">
				<div data-testid="modal-title">{title}</div>
				{titleIcon && <div data-testid="modal-title-icon">{titleIcon}</div>}
				{children}
				{button1Label && (
					<button onClick={button1Handler} data-testid="modal-button-1">
						{button1Label}
					</button>
				)}
				{button2Label && (
					<button onClick={button2Handler} data-testid="modal-button-2">
						{button2Label}
					</button>
				)}
				<button onClick={onClose} data-testid="modal-close-btn">Close</button>
			</div>
		) : null
}));

// Mock ErrorRoundedIcon
jest.mock('@mui/icons-material/ErrorRounded', () => ({
	__esModule: true,
	default: ({ className }: any) => (
		<div data-testid="error-icon" className={className}>ErrorRoundedIcon</div>
	)
}));

describe('DeleteGlossary', () => {
	const defaultProps = {
		open: true,
		onClose: mockOnClose,
		setExpandNode: mockSetExpandNode,
		node: {
			id: 'test-glossary-1',
			guid: 'test-guid-123',
			cGuid: 'test-cguid-456',
			types: 'parent'
		},
		updatedData: mockUpdatedData
	};

	const termNode = {
		id: 'test-term-1',
		guid: 'test-term-guid-123',
		cGuid: 'test-term-cguid-456',
		types: 'term'
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockLocation = {
			pathname: '/glossary',
			search: '',
			hash: '',
			state: null,
			key: 'test-key'
		};
		mockParams = { guid: undefined };
		mockDispatch.mockResolvedValue(undefined);
		mockDeleteGlossaryorTerm.mockResolvedValue({});
		mockDeleteGlossaryorType.mockResolvedValue({});
		mockIsEmpty.mockReturnValue(true);
		mockFetchGlossaryData.mockReturnValue({ type: 'glossary/fetchGlossaryData' });
	});

	afterEach(() => {
		jest.restoreAllMocks();
	});

	describe('Rendering', () => {
		it('should render modal when open is true', () => {
			render(<DeleteGlossary {...defaultProps} />);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			expect(screen.getByTestId('modal-title')).toHaveTextContent('Confirmation');
			expect(screen.getByTestId('error-icon')).toBeInTheDocument();
		});

		it('should not render modal when open is false', () => {
			render(<DeleteGlossary {...defaultProps} open={false} />);

			expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
		});

		it('should display correct text for glossary deletion', () => {
			render(<DeleteGlossary {...defaultProps} />);

			expect(screen.getByText(/Are you sure you want to delete Glossary/i)).toBeInTheDocument();
		});

		it('should display correct text for term deletion', () => {
			render(<DeleteGlossary {...defaultProps} node={termNode} />);

			expect(screen.getByText(/Are you sure you want to delete Term/i)).toBeInTheDocument();
		});

		it('should render Cancel and Ok buttons', () => {
			render(<DeleteGlossary {...defaultProps} />);

			expect(screen.getByTestId('modal-button-1')).toHaveTextContent('Cancel');
			expect(screen.getByTestId('modal-button-2')).toHaveTextContent('Ok');
		});

		it('should render ErrorRoundedIcon with correct className', () => {
			render(<DeleteGlossary {...defaultProps} />);

			const errorIcon = screen.getByTestId('error-icon');
			expect(errorIcon).toHaveClass('remove-modal-icon');
		});
	});

	describe('Modal Interactions', () => {
		it('should call onClose when Cancel button is clicked', () => {
			render(<DeleteGlossary {...defaultProps} />);

			const cancelButton = screen.getByTestId('modal-button-1');
			fireEvent.click(cancelButton);

			expect(mockOnClose).toHaveBeenCalledTimes(1);
		});

		it('should call onClose when close button is clicked', () => {
			render(<DeleteGlossary {...defaultProps} />);

			const closeButton = screen.getByTestId('modal-close-btn');
			fireEvent.click(closeButton);

			expect(mockOnClose).toHaveBeenCalledTimes(1);
		});
	});

	describe('Delete Glossary Functionality', () => {
		it('should delete glossary successfully when Ok button is clicked', async () => {
			mockDispatch.mockResolvedValue(undefined);

			render(<DeleteGlossary {...defaultProps} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockDeleteGlossaryorTerm).toHaveBeenCalledWith('test-guid-123');
				expect(mockDeleteGlossaryorType).not.toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(mockUpdatedData).toHaveBeenCalledTimes(1);
			});

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(mockToastSuccess).toHaveBeenCalledWith('Glossary test-glossary-1 was deleted successfully');
			});

			await waitFor(() => {
				expect(mockOnClose).toHaveBeenCalledTimes(1);
			});

			await waitFor(() => {
				expect(mockSetExpandNode).toHaveBeenCalledWith(null);
			});
		});

		it('should navigate to home when glossaryGuid is not empty after successful deletion', async () => {
			mockParams = { guid: 'existing-guid' };
			mockIsEmpty.mockReturnValue(false);

			render(<DeleteGlossary {...defaultProps} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockNavigate).toHaveBeenCalledWith(
					{ pathname: '/' },
					{ replace: true }
				);
			});
		});

		it('should navigate to home when glossaryType is not empty after successful deletion', async () => {
			mockLocation = {
				pathname: '/glossary',
				search: '?gtype=term',
				hash: '',
				state: null,
				key: 'test-key'
			};
			mockIsEmpty.mockReturnValue(false);

			render(<DeleteGlossary {...defaultProps} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockNavigate).toHaveBeenCalledWith(
					{ pathname: '/' },
					{ replace: true }
				);
			});
		});

		it('should not navigate when both glossaryGuid and glossaryType are empty', async () => {
			mockParams = { guid: undefined };
			mockLocation = {
				pathname: '/glossary',
				search: '',
				hash: '',
				state: null,
				key: 'test-key'
			};
			mockIsEmpty.mockReturnValue(true);

			render(<DeleteGlossary {...defaultProps} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockDeleteGlossaryorTerm).toHaveBeenCalled();
			});

			expect(mockNavigate).not.toHaveBeenCalled();
		});
	});

	describe('Delete Term Functionality', () => {
		it('should delete term successfully when Ok button is clicked', async () => {
			mockDispatch.mockResolvedValue(undefined);

			render(<DeleteGlossary {...defaultProps} node={termNode} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockDeleteGlossaryorType).toHaveBeenCalledWith('test-term-cguid-456');
				expect(mockDeleteGlossaryorTerm).not.toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(mockUpdatedData).toHaveBeenCalledTimes(1);
			});

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(mockToastSuccess).toHaveBeenCalledWith('Term test-term-1 was deleted successfully');
			});

			await waitFor(() => {
				expect(mockOnClose).toHaveBeenCalledTimes(1);
			});

			await waitFor(() => {
				expect(mockSetExpandNode).toHaveBeenCalledWith(null);
			});
		});

		it('should navigate to home when glossaryGuid is not empty after term deletion', async () => {
			mockParams = { guid: 'existing-guid' };
			mockIsEmpty.mockReturnValue(false);

			render(<DeleteGlossary {...defaultProps} node={termNode} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockNavigate).toHaveBeenCalledWith(
					{ pathname: '/' },
					{ replace: true }
				);
			});
		});
	});

	describe('Error Handling', () => {
		it('should handle error when deleting glossary fails', async () => {
			const mockError = new Error('Delete failed');
			mockDeleteGlossaryorTerm.mockRejectedValue(mockError);

			render(<DeleteGlossary {...defaultProps} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockServerError).toHaveBeenCalledWith(mockError, expect.any(Object));
			});

			expect(mockOnClose).not.toHaveBeenCalled();
			expect(mockSetExpandNode).not.toHaveBeenCalled();
		});

		it('should handle error when deleting term fails', async () => {
			const mockError = new Error('Delete failed');
			mockDeleteGlossaryorType.mockRejectedValue(mockError);

			render(<DeleteGlossary {...defaultProps} node={termNode} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockServerError).toHaveBeenCalledWith(mockError, expect.any(Object));
			});

			expect(mockOnClose).not.toHaveBeenCalled();
			expect(mockSetExpandNode).not.toHaveBeenCalled();
		});
	});

	describe('Edge Cases', () => {
		it('should handle node with types as null (treats as term)', () => {
			const nodeWithNullTypes = {
				...defaultProps.node,
				types: null
			};

			render(<DeleteGlossary {...defaultProps} node={nodeWithNullTypes} />);

			expect(screen.getByText(/Are you sure you want to delete Term/i)).toBeInTheDocument();
		});

		it('should handle node with types as undefined (treats as term)', () => {
			const nodeWithUndefinedTypes = {
				...defaultProps.node,
				types: undefined
			};

			render(<DeleteGlossary {...defaultProps} node={nodeWithUndefinedTypes} />);

			expect(screen.getByText(/Are you sure you want to delete Term/i)).toBeInTheDocument();
		});

		it('should handle node with empty string id', async () => {
			const nodeWithEmptyId = {
				...defaultProps.node,
				id: ''
			};

			mockDispatch.mockResolvedValue(undefined);

			render(<DeleteGlossary {...defaultProps} node={nodeWithEmptyId} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockToastSuccess).toHaveBeenCalledWith('Glossary  was deleted successfully');
			});
		});

		it('should handle fetchCurrentData being called', async () => {
			const { fetchGlossaryData } = require('@redux/slice/glossarySlice');
			mockDispatch.mockResolvedValue(undefined);

			render(<DeleteGlossary {...defaultProps} />);

			const okButton = screen.getByTestId('modal-button-2');
			
			await act(async () => {
				fireEvent.click(okButton);
			});

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalled();
			});
		});
	});

	describe('URL Search Params Handling', () => {
		it('should handle search params with gtype', () => {
			mockLocation = {
				pathname: '/glossary',
				search: '?gtype=glossary',
				hash: '',
				state: null,
				key: 'test-key'
			};

			render(<DeleteGlossary {...defaultProps} />);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle search params without gtype', () => {
			mockLocation = {
				pathname: '/glossary',
				search: '?other=value',
				hash: '',
				state: null,
				key: 'test-key'
			};

			render(<DeleteGlossary {...defaultProps} />);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});
	});
});
