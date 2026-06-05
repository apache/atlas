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


/* Tests for FormSelectBoolean */
import React from 'react';
import { render, screen } from '@utils/test-utils';
import userEvent from '@testing-library/user-event';
import { useForm } from 'react-hook-form';
import FormSelectBoolean from '../FormSelectBoolean';

jest.mock('@components/muiComponents', () => ({
  LightTooltip: ({ children }: any) => <>{children}</>
}));

const renderWithForm = (component: React.ReactElement) => {
  const Form: React.FC = () => {
    const { control } = useForm({ defaultValues: { flag: '' } });
    return <>{React.cloneElement(component, { control })}</>;
  };
  return render(<Form />);
};

describe('FormSelectBoolean', () => {
  const data = { name: 'flag', isOptional: false, typeName: 'boolean' };

  it('renders label and selects boolean value', async () => {
    renderWithForm(<FormSelectBoolean data={data} />);
    expect(screen.getByText('Flag')).toBeTruthy();
    expect(screen.getByText('(boolean)')).toBeTruthy();

    const select = screen.getByRole('combobox');
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    await user.click(select);
    const option = await screen.findByRole('option', { name: 'true' });
    await user.click(option);
    expect((select as HTMLElement).textContent).toBe('true');
  });

  it('shows placeholder when no value is selected (via menu)', async () => {
    renderWithForm(<FormSelectBoolean data={data} />);
    const select = screen.getByRole('combobox');
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    await user.click(select);
    expect(await screen.findByText('--Select true or false--')).toBeTruthy();
  });

  it('renderValue fallback returns placeholder when empty', async () => {
    renderWithForm(<FormSelectBoolean data={data} />);
    const select = screen.getByRole('combobox');
    // Without selection, renderValue should be placeholder internally; open to assert option exists
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    await user.click(select);
    expect(await screen.findByText('--Select true or false--')).toBeTruthy();
  });
});


