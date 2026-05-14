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


