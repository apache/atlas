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


/*
 * Tests for FormInputText
 */

import React from 'react';
import { render, screen, fireEvent } from '@utils/test-utils';
import { useForm } from 'react-hook-form';
import FormInputText from '../FormInputText';

jest.mock('@components/muiComponents', () => ({
  LightTooltip: ({ children }: any) => <>{children}</>
}));

const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => <>{children}</>;

const renderWithForm = (component: React.ReactElement) => {
  const Form: React.FC = () => {
    const { control } = useForm({ defaultValues: { name: '' } });
    return <Wrapper>{React.cloneElement(component, { control })}</Wrapper>;
  };
  return render(<Form />);
};

describe('FormInputText', () => {
  const baseData = { name: 'testName', isOptional: false, typeName: 'string' };

  it('renders label, tooltip and handles text input change', () => {
    renderWithForm(<FormInputText data={baseData} />);

    // Label should be capitalized and present
    expect(screen.getByText('TestName')).toBeTruthy();
    // Type hint present
    expect(screen.getByText('(string)')).toBeTruthy();

    const input = screen.getByPlaceholderText('testName') as HTMLInputElement;
    fireEvent.change(input, { target: { value: 'hello' } });
    expect(input.value).toBe('hello');
  });

  it('uses number type when typeName is number', () => {
    renderWithForm(<FormInputText data={{ ...baseData, typeName: 'number' }} />);
    const input = screen.getByPlaceholderText('testName') as HTMLInputElement;
    expect(input.type).toBe('number');
  });
});


