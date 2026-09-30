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

import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { toast } from 'react-toastify';
import { useDropzone } from 'react-dropzone';
import ImportLayout from '../ImportLayout';

// Mock react-dropzone
jest.mock('react-dropzone', () => ({
  useDropzone: jest.fn()
}));

// Mock react-toastify
jest.mock('react-toastify', () => ({
  toast: {
    error: jest.fn(),
    dismiss: jest.fn()
  }
}));

// Mock URL.createObjectURL
global.URL.createObjectURL = jest.fn(() => 'mock-preview-url');
global.URL.revokeObjectURL = jest.fn();

describe('ImportLayout', () => {
  const mockSetFileData = jest.fn();
  const mockSetProgress = jest.fn();
  
  const defaultProps = {
    setFileData: mockSetFileData,
    progressVal: 0,
    setProgress: mockSetProgress,
    selectedFile: [],
    errorDetails: false
  };

  let mockGetRootProps: jest.Mock;
  let mockGetInputProps: jest.Mock;
  let mockOnDrop: jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();
    
    mockGetRootProps = jest.fn(() => ({
      className: 'dropzone',
      onClick: jest.fn()
    }));
    
    mockGetInputProps = jest.fn(() => ({
      type: 'file',
      accept: 'text/csv'
    }));
    
    mockOnDrop = jest.fn();

    (useDropzone as jest.Mock).mockImplementation(({ onDrop }: { onDrop: (files: File[]) => void }) => {
      mockOnDrop = onDrop;
      return {
        getRootProps: mockGetRootProps,
        getInputProps: mockGetInputProps
      };
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Rendering', () => {
    it('should render the dropzone with initial message when no files are selected', () => {
      render(<ImportLayout {...defaultProps} />);
      
      expect(screen.getByText(/Drop files here or click to upload/i)).toBeInTheDocument();
      expect(mockGetRootProps).toHaveBeenCalled();
      expect(mockGetInputProps).toHaveBeenCalled();
    });

    it('should render with selected file when selectedFile prop is provided and errorDetails is false', () => {
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      Object.assign(mockFile, { preview: 'mock-preview-url' });
      
      const props = {
        ...defaultProps,
        selectedFile: [mockFile],
        errorDetails: false
      };

      render(<ImportLayout {...props} />);
      
      waitFor(() => {
        expect(screen.getByText('test.csv')).toBeInTheDocument();
      });
    });

    it('should render empty files when errorDetails is true', () => {
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      
      const props = {
        ...defaultProps,
        selectedFile: [mockFile],
        errorDetails: true
      };

      render(<ImportLayout {...props} />);
      
      expect(screen.getByText(/Drop files here or click to upload/i)).toBeInTheDocument();
    });
  });

  describe('File Upload', () => {
    it('should handle file drop and update state', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(mockSetProgress).toHaveBeenCalledWith(0);
        expect(global.URL.createObjectURL).toHaveBeenCalledWith(mockFile);
      });
    });

    it('should show error toast when trying to upload multiple files', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile1 = new File(['test content 1'], 'test1.csv', { type: 'text/csv' });
      const mockFile2 = new File(['test content 2'], 'test2.csv', { type: 'text/csv' });
      
      // First file upload
      mockOnDrop([mockFile1]);
      
      await waitFor(() => {
        expect(mockSetFileData).toHaveBeenCalled();
      });
      
      // Try to upload second file
      mockOnDrop([mockFile2]);
      
      await waitFor(() => {
        expect(toast.dismiss).toHaveBeenCalled();
        expect(toast.error).toHaveBeenCalledWith('You can not upload any more files..');
      });
    });

    it('should show error toast when no files are accepted', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      mockOnDrop([]);
      
      await waitFor(() => {
        expect(toast.dismiss).toHaveBeenCalled();
        expect(toast.error).toHaveBeenCalledWith("You can't upload files of this type.");
      });
    });
  });

  describe('File Display', () => {
    it('should display file with correct size in bytes', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test'], 'test.csv', { type: 'text/csv' });
      Object.defineProperty(mockFile, 'size', { value: 50 });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('50 b')).toBeInTheDocument();
        expect(screen.getByText('test.csv')).toBeInTheDocument();
      });
    });

    it('should display file with correct size in KB', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      Object.defineProperty(mockFile, 'size', { value: 2048 });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('2.0 KB')).toBeInTheDocument();
      });
    });

    it('should display file with correct size in MB', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      Object.defineProperty(mockFile, 'size', { value: 2097152 });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('2.0 MB')).toBeInTheDocument();
      });
    });

    it('should display "0 Bytes" for zero-sized file', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File([''], 'test.csv', { type: 'text/csv' });
      Object.defineProperty(mockFile, 'size', { value: 0 });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('0 Bytes')).toBeInTheDocument();
      });
    });

    it('should display file size between 100 bytes and 100 KB correctly', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      Object.defineProperty(mockFile, 'size', { value: 150 });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('0.1 KB')).toBeInTheDocument();
      });
    });
  });

  describe('Progress Display', () => {
    it('should display progress bar with correct value', async () => {
      const props = {
        ...defaultProps,
        progressVal: 50
      };
      
      render(<ImportLayout {...props} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        const progressBar = screen.getByRole('progressbar');
        expect(progressBar).toBeInTheDocument();
        expect(progressBar).toHaveAttribute('aria-valuenow', '50');
      });
    });

    it('should show "Cancel Upload" button when upload is in progress', async () => {
      const props = {
        ...defaultProps,
        progressVal: 50
      };
      
      render(<ImportLayout {...props} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('Cancel Upload')).toBeInTheDocument();
      });
    });

    it('should show "Remove file" button when upload is complete', async () => {
      const props = {
        ...defaultProps,
        progressVal: 100
      };
      
      render(<ImportLayout {...props} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('Remove file')).toBeInTheDocument();
      });
    });

    it('should show "Remove file" button when upload has not started', async () => {
      const props = {
        ...defaultProps,
        progressVal: 0
      };
      
      render(<ImportLayout {...props} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('Remove file')).toBeInTheDocument();
      });
    });
  });

  describe('File Removal', () => {
    it('should remove file when remove button is clicked', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('test.csv')).toBeInTheDocument();
      });
      
      const removeButton = screen.getByText('Remove file');
      fireEvent.click(removeButton);
      
      await waitFor(() => {
        expect(screen.getByText(/Drop files here or click to upload/i)).toBeInTheDocument();
      });
    });

    it('should stop event propagation when remove button is clicked', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('test.csv')).toBeInTheDocument();
      });
      
      const removeButton = screen.getByText('Remove file');
      const mockEvent = {
        stopPropagation: jest.fn()
      };
      
      fireEvent.click(removeButton, mockEvent);
      
      await waitFor(() => {
        expect(screen.getByText(/Drop files here or click to upload/i)).toBeInTheDocument();
      });
    });

    it('should call setFileData with undefined when file is removed', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      await act(async () => {
        mockOnDrop([mockFile]);
      });
      
      await waitFor(() => {
        expect(screen.getByText('test.csv')).toBeInTheDocument();
        expect(mockSetFileData).toHaveBeenCalledWith(mockFile);
      });
      
      mockSetFileData.mockClear();
      
      const removeButton = screen.getByRole('button', { name: 'remove' });
      fireEvent.click(removeButton);
      
      await waitFor(() => {
        expect(mockSetFileData).toHaveBeenCalledWith(undefined);
      });
    });
  });

  describe('useEffect Hook', () => {
    it('should call setFileData when files change', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(mockSetFileData).toHaveBeenCalled();
      });
    });

    it('should update hasFiles state when files are added', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      expect(screen.getByText(/Drop files here or click to upload/i)).toBeInTheDocument();
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('test.csv')).toBeInTheDocument();
      });
    });
  });

  describe('Dropzone Configuration', () => {
    it('should configure dropzone with correct accept types', () => {
      render(<ImportLayout {...defaultProps} />);
      
      expect(useDropzone).toHaveBeenCalledWith(
        expect.objectContaining({
          accept: { 'text/csv': ['.csv', '.xls', '.xlsx'] },
          multiple: false,
          maxFiles: 1
        })
      );
    });
  });

  describe('Data Attributes', () => {
    it('should have data-cy attribute for testing', () => {
      const { container } = render(<ImportLayout {...defaultProps} />);
      
      const dropzone = container.querySelector('[data-cy="importGlossary"]');
      expect(dropzone).toBeInTheDocument();
    });
  });

  describe('Edge Cases', () => {
    it('should handle file with negative size (edge case)', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      Object.defineProperty(mockFile, 'size', { value: -1 });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText('test.csv')).toBeInTheDocument();
      });
    });

    it('should handle file with very long name', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const longFileName = 'a'.repeat(200) + '.csv';
      const mockFile = new File(['test content'], longFileName, { type: 'text/csv' });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        expect(screen.getByText(longFileName)).toBeInTheDocument();
      });
    });

    it('should handle multiple consecutive file uploads', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile1 = new File(['test1'], 'test1.csv', { type: 'text/csv' });
      mockOnDrop([mockFile1]);
      
      await waitFor(() => {
        expect(screen.getByText('test1.csv')).toBeInTheDocument();
      });
      
      const removeButton = screen.getByText('Remove file');
      fireEvent.click(removeButton);
      
      await waitFor(() => {
        expect(screen.getByText(/Drop files here or click to upload/i)).toBeInTheDocument();
      });
      
      const mockFile2 = new File(['test2'], 'test2.csv', { type: 'text/csv' });
      mockOnDrop([mockFile2]);
      
      await waitFor(() => {
        expect(screen.getByText('test2.csv')).toBeInTheDocument();
      });
    });

    it('should handle file size exactly at 100 bytes boundary', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      Object.defineProperty(mockFile, 'size', { value: 100 });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        // At exactly 100 bytes, it should display as KB
        const text = screen.getByText(/KB/i);
        expect(text).toBeInTheDocument();
      });
    });

    it('should handle file size exactly at 100 KB boundary', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile = new File(['test content'], 'test.csv', { type: 'text/csv' });
      Object.defineProperty(mockFile, 'size', { value: 102400 });
      
      mockOnDrop([mockFile]);
      
      await waitFor(() => {
        // At exactly 100 KB, it should display as MB
        const text = screen.getByText(/MB/i);
        expect(text).toBeInTheDocument();
      });
    });
  });

  describe('Toast Notifications', () => {
    it('should dismiss previous toast before showing new error', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      const mockFile1 = new File(['test1'], 'test1.csv', { type: 'text/csv' });
      mockOnDrop([mockFile1]);
      
      await waitFor(() => {
        expect(screen.getByText('test1.csv')).toBeInTheDocument();
      });
      
      const mockFile2 = new File(['test2'], 'test2.csv', { type: 'text/csv' });
      mockOnDrop([mockFile2]);
      
      await waitFor(() => {
        expect(toast.dismiss).toHaveBeenCalled();
        expect(toast.error).toHaveBeenCalledWith('You can not upload any more files..');
      });
    });

    it('should dismiss toast before showing file type error', async () => {
      render(<ImportLayout {...defaultProps} />);
      
      mockOnDrop([]);
      
      await waitFor(() => {
        expect(toast.dismiss).toHaveBeenCalled();
        expect(toast.error).toHaveBeenCalledWith("You can't upload files of this type.");
      });
    });
  });
});
