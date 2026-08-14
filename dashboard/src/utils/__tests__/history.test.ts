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
 * Unit tests for history.ts
 * 
 * Coverage Target: 100% for Statements, Branches, Functions, and Lines
 */

import React from 'react';
import { renderHook } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import useHistory from '../history';

// Mock react-router-dom
const mockNavigate = jest.fn();
jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useNavigate: () => mockNavigate
}));

describe('useHistory', () => {
	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('should return push function that calls navigate', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		result.current.push('/test/path');
		expect(mockNavigate).toHaveBeenCalledWith('/test/path');
	});

	it('should return replace function that calls navigate with replace option', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		result.current.replace('/test/path');
		expect(mockNavigate).toHaveBeenCalledWith('/test/path', { replace: true });
	});

	it('should return goBack function that calls navigate with -1', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		result.current.goBack();
		expect(mockNavigate).toHaveBeenCalledWith(-1);
	});

	it('should return all three functions', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		expect(typeof result.current.push).toBe('function');
		expect(typeof result.current.replace).toBe('function');
		expect(typeof result.current.goBack).toBe('function');
	});

	it('should call push multiple times', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		result.current.push('/path1');
		result.current.push('/path2');
		expect(mockNavigate).toHaveBeenCalledTimes(2);
		expect(mockNavigate).toHaveBeenNthCalledWith(1, '/path1');
		expect(mockNavigate).toHaveBeenNthCalledWith(2, '/path2');
	});

	it('should call replace multiple times', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		result.current.replace('/path1');
		result.current.replace('/path2');
		expect(mockNavigate).toHaveBeenCalledTimes(2);
		expect(mockNavigate).toHaveBeenNthCalledWith(1, '/path1', { replace: true });
		expect(mockNavigate).toHaveBeenNthCalledWith(2, '/path2', { replace: true });
	});

	it('should call goBack multiple times', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		result.current.goBack();
		result.current.goBack();
		expect(mockNavigate).toHaveBeenCalledTimes(2);
		expect(mockNavigate).toHaveBeenNthCalledWith(1, -1);
		expect(mockNavigate).toHaveBeenNthCalledWith(2, -1);
	});

	it('should handle different path formats in push', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		result.current.push('/');
		result.current.push('/test');
		result.current.push('/test/nested/path');
		expect(mockNavigate).toHaveBeenCalledTimes(3);
	});

	it('should handle different path formats in replace', () => {
		const wrapper = ({ children }: { children: React.ReactNode }) => {
			return React.createElement(BrowserRouter, null, children);
		};

		const { result } = renderHook(() => useHistory(), { wrapper });

		result.current.replace('/');
		result.current.replace('/test');
		result.current.replace('/test/nested/path');
		expect(mockNavigate).toHaveBeenCalledTimes(3);
	});
});
