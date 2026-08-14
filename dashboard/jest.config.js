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
 * Jest configuration for the Atlas React dashboard
 */

const runDependencyGovernance = process.env.RUN_DEPENDENCY_GOVERNANCE === '1'

const config = {
  preset: 'ts-jest/presets/js-with-ts',
  testEnvironment: 'jsdom',
  
  // Test file patterns
  testMatch: [
    '<rootDir>/src/**/__tests__/**/*.{ts,tsx}',
    '<rootDir>/src/**/*.{test,spec}.{ts,tsx}'
  ],
  
  // Setup files
  setupFilesAfterEnv: ['<rootDir>/src/setupTests.simple.ts'],
  
  // Module name mapping for path aliases
  moduleNameMapper: {
    '^@/(.*)$': '<rootDir>/src/$1',
    '^@components/(.*)$': '<rootDir>/src/components/$1',
    '^@api/(.*)\.js$': '<rootDir>/src/api/$1.ts',
    '^@api/(.*)$': '<rootDir>/src/api/$1',
    '^@utils/(.*)\.js$': '<rootDir>/src/utils/$1.ts',
    '^@utils/(.*)$': '<rootDir>/src/utils/$1',
    '^@styles/(.*)$': '<rootDir>/src/styles/$1',
    '^@services/(.*)$': '<rootDir>/src/services/$1',
    '^@views/(.*)$': '<rootDir>/src/views/$1',
    '^@hooks/(.*)$': '<rootDir>/src/hooks/$1',
    '^@models/(.*)$': '<rootDir>/src/models/$1',
    '^@contexts/(.*)$': '<rootDir>/src/contexts/$1',
    '^@redux/(.*)$': '<rootDir>/src/redux/$1',
    '\\.(css|less|scss|sass)$': '<rootDir>/src/__mocks__/styleMock.ts',
    '\\.(jpg|jpeg|png|gif|svg|webp)$': '<rootDir>/src/__mocks__/fileMock.ts'
  },
  
  // File extensions to consider
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json'],
  
  // Transform files (updated ts-jest config format)
  transform: {
    '^.+\\.(ts|tsx)$': ['ts-jest', {
      tsconfig: 'tsconfig.test.json'
    }],
    '^.+\\.(js|jsx|mjs|cjs)$': 'babel-jest'
  },
  
  // Files to ignore during transformation
  // Allow transpiling ESM-only d3 transitive deps for jest.requireActual('d3')
  transformIgnorePatterns: [
    'node_modules/(?!(@testing-library|@adobe/css-tools|react-quill-new|d3|d3-[-\\w]+|internmap|.*\\.mjs$))'
  ],
  
  // Module paths to ignore (exclude bin/, dist/, build/, target/)
  modulePathIgnorePatterns: [
    '<rootDir>/bin/',
    '<rootDir>/dist/',
    '<rootDir>/build/',
    '<rootDir>/target/'
  ],

  testPathIgnorePatterns: [
    '<rootDir>/bin/',
    '<rootDir>/dist/',
    '<rootDir>/build/',
    '<rootDir>/target/',
    ...(runDependencyGovernance
      ? []
      : ['<rootDir>/src/__tests__/governance/'])
  ],
  
  // V8 avoids babel-plugin-istanbul + test-exclude, which breaks when npm
  // overrides force minimatch@9 (minimatch is not a function in CJS).
  coverageProvider: 'v8',

  // Coverage configuration (only collect when --coverage flag is used)
  collectCoverage: false,
  collectCoverageFrom: [
    'src/**/*.{ts,tsx}',
    '!src/**/*.d.ts',
    '!src/**/__tests__/**',
    '!src/**/*.test.{ts,tsx}',
    '!src/**/*.spec.{ts,tsx}',
    '!src/**/__mocks__/**',
    '!src/**/setupTests*',
    '!src/**/test-utils*'
  ],
  
  coverageDirectory: 'coverage',
  
  coverageReporters: [
    'text',
    'lcov',
    'html',
    'json-summary'
  ],
  
  // Coverage thresholds
  coverageThreshold: {
    global: {
      branches: 50,
      functions: 50,
      lines: 50,
      statements: 50
    }
  },
  
  // Verbose output
  verbose: true,
  
  // Clear mocks between tests
  clearMocks: true,
  
  // Reset modules between tests
  resetMocks: false,
  
  // Environment variables
  testEnvironmentOptions: {
    url: 'http://localhost:3000'
  },
  
  // Test timeout (prevent hanging tests)
  testTimeout: 10000, // 10 seconds
  
  // Maximum number of concurrent workers
  maxWorkers: '50%',
  
  // Bail on first failure (stop after first failing test suite)
  bail: false,
  
  // Force exit after tests complete (prevent hanging)
  forceExit: true,
  
  // Detect open handles that might prevent Jest from exiting
  detectOpenHandles: false
};

export default config;