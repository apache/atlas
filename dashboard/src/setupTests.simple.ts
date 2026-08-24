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

/** Simplified test setup file for Node 12 compatibility */

import '@testing-library/jest-dom';
import { TextEncoder, TextDecoder } from 'util';

export {};

// Polyfill TextEncoder and TextDecoder for React Router DOM in Jest
if (typeof global.TextEncoder === 'undefined') {
  global.TextEncoder = TextEncoder;
  global.TextDecoder = TextDecoder as unknown as typeof global.TextDecoder;
}


// Basic mocks that don't rely on newer JS features
Object.assign(global, {
  ResizeObserver: class ResizeObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
});

Object.assign(global, {
  IntersectionObserver: class IntersectionObserver {
    root = null;
    rootMargin = '';
    thresholds = [];
    observe() {}
    unobserve() {}
    disconnect() {}
    takeRecords() { return []; }
  }
});

// Mock window.matchMedia (jsdom / browser tests only)
if (typeof window !== 'undefined') {
	Object.defineProperty(window, 'matchMedia', {
		writable: true,
		value: function () {
			return {
				matches: false,
				media: '',
				onchange: null,
				addListener: function () {},
				removeListener: function () {},
				addEventListener: function () {},
				removeEventListener: function () {},
				dispatchEvent: function () {},
			}
		},
	})
}

jest.mock('@components/Table/TableLayout', () => ({
	__esModule: true,
	...jest.requireActual('./__mocks__/table-layout-mock')
}))