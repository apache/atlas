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


/* Tests for FormSingleSelect */
import React from 'react';
import { render, screen } from '@utils/test-utils';
import userEvent from '@testing-library/user-event';
import { useForm } from 'react-hook-form';
import FormSingleSelect from '../FormSingleSelect';

const renderWithForm = (component: React.ReactElement) => {
  const Form: React.FC = () => {
    const { control } = useForm({ defaultValues: { choice: '' } });
    return <>{React.cloneElement(component, { control })}</>;
  };
  return render(<Form />);
};

describe('FormSingleSelect', () => {
  const data = { name: 'choice', isOptional: false } as any;
  const optionsList = [{ value: 'A' }, { value: 'B' }];

  it('renders options and selects a value', async () => {
    renderWithForm(
      <FormSingleSelect data={data} optionsList={optionsList} typeName="string" />
    );
    expect(screen.getByText('Choice')).toBeTruthy();
    const select = screen.getByRole('combobox');
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    await user.click(select);
    const option = await screen.findByRole('option', { name: 'A' });
    await user.click(option);
    expect((select as HTMLElement).textContent).toBe('A');
  });

  it('shows placeholder before selection (via menu)', async () => {
    renderWithForm(
      <FormSingleSelect data={data} optionsList={optionsList} typeName="string" />
    );
    const select = screen.getByRole('combobox');
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    await user.click(select);
    expect(await screen.findByText('--Select string--')).toBeTruthy();
  });

  it('renderValue fallback returns placeholder when empty', async () => {
    renderWithForm(
      <FormSingleSelect data={data} optionsList={optionsList} typeName="string" />
    );
    const select = screen.getByRole('combobox');
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    await user.click(select);
    expect(await screen.findByText('--Select string--')).toBeTruthy();
  });
});


