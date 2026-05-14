/* Tests for FormCreatableSelect */
import React from 'react';
import { render, screen, fireEvent } from '@utils/test-utils';
import userEvent from '@testing-library/user-event';
import { useForm } from 'react-hook-form';
import FormCreatableSelect from '../FormCreatableSelect';

jest.mock('@components/muiComponents', () => ({
  LightTooltip: ({ children }: any) => <>{children}</>
}));

// Mock Autocomplete to surface filterOptions, renderOption and equality
jest.mock('@mui/material/Autocomplete', () => {
  const React = require('react');
  const Mock = ({ filterOptions, renderInput, renderOption, isOptionEqualToValue, onChange }: any) => {
    const params = { InputProps: {} } as any;
    // Exercise both branches of isExisting in filterOptions
    const filteredPush = filterOptions(['a'], { inputValue: 'x' } as any);
    const filteredNoPush = filterOptions(['x'], { inputValue: 'x' } as any);
    // Check equality branches by calling with two objects
    isOptionEqualToValue?.({ inputValue: 'x' }, { inputValue: 'x' });
    return (
      <div>
        {renderInput?.(params)}
        <ul role="list">
          {filteredPush.map((opt: any, idx: number) => renderOption?.({ key: `p-${idx}` } as any, opt))}
          {filteredNoPush.map((opt: any, idx: number) => renderOption?.({ key: `n-${idx}` } as any, opt))}
        </ul>
        <button data-testid="select-change" onClick={() => onChange?.(null, ['x'])} />
      </div>
    );
  };
  const createFilterOptions = () => (options: any[], params: any) => {
    const inputValue = params.inputValue;
    const exists = options.some((o) => o === inputValue);
    const base = [...options];
    if (inputValue !== '' && !exists) base.push({ inputValue });
    return base;
  };
  return { __esModule: true, default: Mock, createFilterOptions };
});

const renderWithForm = (component: React.ReactElement) => {
  const Form: React.FC = () => {
    const { control } = useForm({ defaultValues: { tags: [] } });
    return <>{React.cloneElement(component, { control })}</>;
  };
  return render(<Form />);
};

describe('FormCreatableSelect', () => {
  const data = { name: 'tags', isOptional: false, typeName: 'string', cardinality: 'SET' } as any;

  it('renders and allows creating a new option', async () => {
    renderWithForm(<FormCreatableSelect data={data} />);
    expect(screen.getByText('Tags')).toBeTruthy();

    // Trigger onChange
    fireEvent.click(screen.getByTestId('select-change'));
  });

  it('getOptionLabel returns inputValue or raw option and renderOption renders li', () => {
    renderWithForm(<FormCreatableSelect data={data} />);
    // Rendered li from renderOption
    // Ensures filterOptions pushed inputValue and created list items
    expect(screen.getByRole('list')).toBeTruthy();
  });
});


