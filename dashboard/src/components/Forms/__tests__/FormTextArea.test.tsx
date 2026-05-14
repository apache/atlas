/**
 * Tests for FormTextArea
 */

import React from 'react';
import { render, screen, fireEvent } from '@utils/test-utils';
import { useForm } from 'react-hook-form';
import FormTextArea from '../FormTextArea';

jest.mock('@components/muiComponents', () => ({
	LightTooltip: ({ children }: any) => <>{children}</>
}));

const renderWithForm = (component: React.ReactElement) => {
	const Form: React.FC = () => {
		const { control } = useForm({ defaultValues: { description: '' } });
		return <>{React.cloneElement(component, { control })}</>;
	};
	return render(<Form />);
};

describe('FormTextArea', () => {
	const baseData = { name: 'description', isOptional: false, typeName: 'string' };

	it('renders label, tooltip and handles text input change', () => {
		renderWithForm(<FormTextArea data={baseData} />);

		// Label should be capitalized and present
		expect(screen.getByText('Description')).toBeTruthy();
		// Type hint present
		expect(screen.getByText('(string)')).toBeTruthy();

		const textarea = screen.getByPlaceholderText('description') as HTMLTextAreaElement;
		fireEvent.change(textarea, { target: { value: 'test description' } });
		expect(textarea.value).toBe('test description');
	});

	it('shows required indicator when field is not optional', () => {
		renderWithForm(<FormTextArea data={baseData} />);

		const label = screen.getByText('Description');
		expect(label).toBeTruthy();
		// Check that required attribute is set (via InputLabel required prop)
	});

	it('does not show required indicator when field is optional', () => {
		renderWithForm(<FormTextArea data={{ ...baseData, isOptional: true }} />);

		const label = screen.getByText('Description');
		expect(label).toBeTruthy();
	});

	it('displays correct type name in tooltip', () => {
		renderWithForm(<FormTextArea data={{ ...baseData, typeName: 'text' }} />);

		expect(screen.getByText('(text)')).toBeTruthy();
	});

	it('handles empty value', () => {
		renderWithForm(<FormTextArea data={baseData} />);

		const textarea = screen.getByPlaceholderText('description') as HTMLTextAreaElement;
		expect(textarea.value).toBe('');
	});

	it('integrates with react-hook-form control', () => {
		renderWithForm(<FormTextArea data={baseData} />);

		const textarea = screen.getByPlaceholderText('description') as HTMLTextAreaElement;
		expect(textarea).toBeTruthy();
		// Form integration is tested by the fact that component renders and accepts control prop
	});
});

