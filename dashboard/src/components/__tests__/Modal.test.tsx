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
 * Unit tests for CustomModal component
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CustomModal from '../Modal';

// Create a theme for testing
const theme = createTheme();

// Wrapper component to provide theme
const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('CustomModal', () => {
  const defaultProps = {
    open: true,
    onClose: jest.fn(),
    title: 'Test Modal',
    button1Label: 'Cancel',
    button1Handler: jest.fn(),
    button2Label: 'Save',
    button2Handler: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should render modal when open is true', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} />
      </TestWrapper>
    );

    expect(!!screen.getByRole('dialog')).toBe(true);
    expect(!!screen.getByText('Test Modal')).toBe(true);
  });

  it('should not render modal when open is false', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} open={false} />
      </TestWrapper>
    );

    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('should display the correct title', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} title="Custom Title" />
      </TestWrapper>
    );

    expect(!!screen.getByText('Custom Title')).toBe(true);
  });

  it('should render children content', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps}>
          <div data-testid="modal-content">Test Content</div>
        </CustomModal>
      </TestWrapper>
    );

    expect(!!screen.getByTestId('modal-content')).toBe(true);
    expect(!!screen.getByText('Test Content')).toBe(true);
  });

  it('should call onClose when close button is clicked', async () => {
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} />
      </TestWrapper>
    );

    const closeIcon = screen.getAllByTestId('CloseIcon')[0];
    const closeButton = closeIcon?.closest('button');
    if (closeButton) {
      await user.click(closeButton);
    }

    expect(defaultProps.onClose).toHaveBeenCalledTimes(1);
  });

  it('should render both action buttons with correct labels', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} />
      </TestWrapper>
    );

    expect(!!screen.getByText('Cancel')).toBe(true);
    expect(!!screen.getByText('Save')).toBe(true);
  });

  it('should call button1Handler when first button is clicked', async () => {
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} />
      </TestWrapper>
    );

    await user.click(screen.getByText('Cancel'));
    expect(defaultProps.button1Handler).toHaveBeenCalledTimes(1);
  });

  it('should call button2Handler when second button is clicked', async () => {
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} />
      </TestWrapper>
    );

    await user.click(screen.getByText('Save'));
    expect(defaultProps.button2Handler).toHaveBeenCalledTimes(1);
  });

  it('should disable second button when disableButton2 is true', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} disableButton2={true} />
      </TestWrapper>
    );

    const saveButton = screen.getByText('Save');
    expect((saveButton as HTMLButtonElement).disabled).toBe(true);
  });

  it('should not render first button when button1Label is empty', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} button1Label="" />
      </TestWrapper>
    );

    expect(screen.queryByText('Cancel')).toBeNull();
    expect(!!screen.getByText('Save')).toBe(true);
  });

  it('should disable second button based on isDirty prop', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} isDirty={false} />
      </TestWrapper>
    );

    const saveButton = screen.getByText('Save');
    expect((saveButton as HTMLButtonElement).disabled).toBe(true);
  });

  it('should enable second button when isDirty is true', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} isDirty={true} />
      </TestWrapper>
    );

    const saveButton = screen.getByText('Save');
    expect((saveButton as HTMLButtonElement).disabled).toBe(false);
  });



  it('should render title icons when provided', () => {
    render(
      <TestWrapper>
        <CustomModal
          {...defaultProps}
          titleIcon={<span data-testid="title-icon">icon</span>}
          postTitleIcon={<span data-testid="post-title-icon">post</span>}
        />
      </TestWrapper>
    )

    expect(!!screen.getByTestId('title-icon')).toBe(true)
    expect(!!screen.getByTestId('post-title-icon')).toBe(true)
  })

  it('should not show progressbar when isDirty is false', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} disableButton2={true} isDirty={false} />
      </TestWrapper>
    )

    expect(screen.queryByRole('progressbar')).toBeNull()
  })

  it('should show loading indicator when button2Loading is true', () => {
    render(
      <TestWrapper>
        <CustomModal
          {...defaultProps}
          disableButton2={false}
          button2Loading={true}
          isDirty={true}
        />
      </TestWrapper>
    );

    expect(
      screen.getByRole('progressbar', { hidden: true })
    ).toBeInTheDocument();
  });

  it('should not render footer when footer prop is false', () => {
    render(
      <TestWrapper>
        <CustomModal {...defaultProps} footer={false} />
      </TestWrapper>
    );

    // The buttons should not be present when footer is false
    expect(screen.queryByText('Cancel')).toBeNull();
    expect(screen.queryByText('Save')).toBeNull();
  });

  // The stopPropagation behavior relies on React's synthetic event system; covered indirectly
});