/**
 * Simplified test setup file for Node 12 compatibility
 */

import '@testing-library/jest-dom';

export {};


// Basic mocks that don't rely on newer JS features
(global as any).ResizeObserver = function() {
  return {
    observe: function() {},
    unobserve: function() {},
    disconnect: function() {}
  };
};

(global as any).IntersectionObserver = function() {
  return {
    observe: function() {},
    unobserve: function() {},
    disconnect: function() {},
    takeRecords: function() { return []; },
    root: null,
    rootMargin: '',
    thresholds: []
  };
};

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