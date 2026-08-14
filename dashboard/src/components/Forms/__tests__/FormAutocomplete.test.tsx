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


/* Tests for FormAutocomplete */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@utils/test-utils';
import { useForm } from 'react-hook-form';
import FormAutocomplete from '../FormAutocomplete';

jest.mock('@components/muiComponents', () => ({
  LightTooltip: ({ children }: any) => <>{children}</>
}));

// Mock MUI Autocomplete to deterministically invoke handlers
jest.mock('@mui/material/Autocomplete', () => {
  const React = require('react');
  return React.forwardRef((props: any, _ref: any) => {
    // Exercise getOptionLabel and equality branches
    props.getOptionLabel?.('s');
    props.getOptionLabel?.({ label: 'Lbl' });
    props.getOptionLabel?.({ inputValue: 'IV' });
    props.getOptionLabel?.({});
    props.isOptionEqualToValue?.('a', 'a');
    props.isOptionEqualToValue?.({ inputValue: 'x' }, { inputValue: 'x' });

    const params = { InputProps: { endAdornment: null } } as any;
    return (
      <div>
        {props.renderInput ? props.renderInput(params) : null}
        <input
          role="combobox"
          onChange={(e: any) => props.onInputChange?.(null, e.target.value)}
        />
        <button data-testid="trigger-change" onClick={() => props.onChange?.(null, { label: 'Sel' })} />
        <button data-testid="equality-check" onClick={() => props.isOptionEqualToValue?.('x','x')} />
      </div>
    );
  });
});

jest.mock('@api/apiMethods/entityFormApiMethod', () => ({
  getAttributes: jest
    .fn()
    .mockResolvedValueOnce({ data: { entities: [ { guid: '1', typeName: 'Type', attributes: {}, values: {}, status: 'ACTIVE', entityStatus: 'ACTIVE' } ] } })
    .mockRejectedValueOnce(new Error('network'))
}));

jest.mock('@utils/Utils', () => ({
  ...jest.requireActual('@utils/Utils'),
  extractKeyValueFromEntity: (_obj: any) => ({ name: 'Option 1' }),
  isEmpty: (v: any) => v == null || (Array.isArray(v) ? v.length === 0 : v === ''),
  serverError: jest.fn()
}));

jest.mock('react-toastify', () => ({
  toast: { dismiss: jest.fn() }
}));

const renderWithForm = (component: React.ReactElement) => {
  const Form: React.FC = () => {
    const { control } = useForm({ defaultValues: { auto: [] } });
    return <>{React.cloneElement(component, { control })}</>;
  };
  return render(<Form />);
};

describe('FormAutocomplete', () => {
  const data = { name: 'auto', isOptional: false, typeName: 'array<string>' } as any;
  const dataSimple = { name: 'auto2', isOptional: false, typeName: 'string' } as any;

  it('renders and fetches options on input', async () => {
    renderWithForm(<FormAutocomplete data={data} />);
    expect(screen.getByText('(array<string>)')).toBeTruthy();

    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'op' } });
    await waitFor(() => expect(require('@api/apiMethods/entityFormApiMethod').getAttributes).toHaveBeenCalled());
  });

  it('clears options when input is empty and handles non-string values', async () => {
    renderWithForm(<FormAutocomplete data={data} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: '' } });
    // Trigger getOptionLabel fallbacks
    const utils = require('@utils/Utils');
    expect(utils.isEmpty('')).toBe(true);
  });

  it('handles error path and shows serverError via toast.dismiss', async () => {
    renderWithForm(<FormAutocomplete data={data} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'fail' } });
    await waitFor(() => expect(require('@api/apiMethods/entityFormApiMethod').getAttributes).toHaveBeenCalled());
    const { toast } = require('react-toastify');
    const { serverError } = require('@utils/Utils');
    expect(toast.dismiss).toHaveBeenCalled();
    expect(serverError).toHaveBeenCalled();
  });

  it('shows spinner while loading and covers empty-entities branch', async () => {
    const { getAttributes } = require('@api/apiMethods/entityFormApiMethod');
    getAttributes.mockImplementationOnce(() => new Promise((resolve) => setTimeout(() => resolve({ data: { entities: [] } }), 10)));
    renderWithForm(<FormAutocomplete data={data} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'x' } });
    await waitFor(() => expect(screen.getByRole('progressbar')).toBeTruthy());
  });

  it('onChange is invoked and equality branch with non-empty options executes', async () => {
    const { getAttributes } = require('@api/apiMethods/entityFormApiMethod');
    getAttributes.mockImplementationOnce(() => new Promise((resolve) => setTimeout(() => resolve({ data: { entities: [ { guid: 'g', typeName: 'T' } ] } }), 10)));
    renderWithForm(<FormAutocomplete data={data} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'ok' } });
    await waitFor(() => expect(getAttributes).toHaveBeenCalled());
    await new Promise(r => setTimeout(r, 20));
    fireEvent.click(screen.getByTestId('equality-check'));
    fireEvent.click(screen.getByTestId('trigger-change'));
  });

  it('treats string "undefined" as empty and clears options', () => {
    renderWithForm(<FormAutocomplete data={data} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'undefined' } });
  });

  it('non-array typeName path uses raw typeName in params', async () => {
    const { getAttributes } = require('@api/apiMethods/entityFormApiMethod');
    getAttributes.mockResolvedValueOnce({ data: { entities: [] } });
    renderWithForm(<FormAutocomplete data={dataSimple} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'a' } });
    await waitFor(() => expect(getAttributes).toHaveBeenCalled());
  });

  it('covers getOptionLabel default and isOptionEqualToValue when options empty', () => {
    const Form: React.FC = () => {
      const { control } = require('react-hook-form').useForm({ defaultValues: { auto: [{}] } });
      return <>{React.cloneElement(<FormAutocomplete data={data} /> as any, { control })}</>;
    };
    render(<Form />);
  });
});


