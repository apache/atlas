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

import eslint from '@eslint/js';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default tseslint.config(
	{
		ignores: [
			'dist/**',
			'target/**',
			'coverage/**',
			'node_modules/**',
			'.frontend-toolchain/**',
			'src/views/Lineage/atlas-lineage/dist/**',
		],
	},
	eslint.configs.recommended,
	...tseslint.configs.recommended,
	{
		files: ['**/*.{ts,tsx}'],
		languageOptions: {
			ecmaVersion: 2020,
			globals: globals.browser,
			parser: tseslint.parser,
			parserOptions: {
				ecmaFeatures: { jsx: true },
			},
		},
		plugins: {
			'react-hooks': reactHooks,
			'react-refresh': reactRefresh,
		},
		rules: {
			// react-hooks v7 "recommended" adds React Compiler rules; keep v4-era hooks lint only
			'react-hooks/rules-of-hooks': 'error',
			'react-hooks/exhaustive-deps': 'warn',
			'react-refresh/only-export-components': [
				'warn',
				{ allowConstantExport: true },
			],
			'@typescript-eslint/no-explicit-any': 'off',
			'@typescript-eslint/no-unused-expressions': 'off',
			'@typescript-eslint/no-this-alias': 'warn',
			'no-undef': 'off',
			'no-unused-vars': 'off',
			'no-unused-expressions': 'off',
			'no-useless-assignment': 'off',
			'prefer-const': 'off',
			'no-var': 'off',
			'no-useless-catch': 'warn',
			'@typescript-eslint/ban-ts-comment': 'warn',
			'@typescript-eslint/no-empty-object-type': 'warn',
			'@typescript-eslint/no-loss-of-precision': 'warn',
			'no-loss-of-precision': 'warn',
			'no-constant-binary-expression': 'warn',
			'@typescript-eslint/no-unused-vars': [
				'warn',
				{
					argsIgnorePattern: '^_',
					varsIgnorePattern: '^_',
					caughtErrorsIgnorePattern: '^_',
				},
			],
			'no-case-declarations': 'warn',
			'no-empty': ['warn', { allowEmptyCatch: true }],
			'no-inner-declarations': 'warn',
			'no-prototype-builtins': 'warn',
			'no-self-assign': 'warn',
			'no-sparse-arrays': 'warn',
			'prefer-rest-params': 'warn',
		},
	},
	{
		files: ['scripts/**/*.{js,mjs}'],
		languageOptions: {
			ecmaVersion: 2020,
			globals: globals.node,
		},
	},
	{
		files: [
			'**/__tests__/**/*.{ts,tsx}',
			'**/*.{test,spec}.{ts,tsx}',
			'**/setupTests*.ts',
			'src/**/__mocks__/**/*.{ts,tsx}',
		],
		languageOptions: {
			globals: globals.jest,
		},
		rules: {
			'@typescript-eslint/no-require-imports': 'off',
			'@typescript-eslint/no-var-requires': 'off',
			'@typescript-eslint/no-unused-vars': 'off',
			'@typescript-eslint/no-empty-object-type': 'off',
			'@typescript-eslint/no-loss-of-precision': 'off',
			'react-hooks/rules-of-hooks': 'off',
			'react-hooks/exhaustive-deps': 'off',
		},
	},
);
