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

import {
	getGlossaryImport,
	getGlossaryImportTmpl
} from '@api/apiMethods/glossaryApiMethod';

/**
 * Same behavior as glossary branch of SideBarTree.downloadFile.
 */
export const downloadGlossaryImportTemplate = async (): Promise<void> => {
	const apiResp = await getGlossaryImportTmpl({});
	const text =
		apiResp && typeof apiResp.data !== 'undefined'
			? String(apiResp.data)
			: '';
	const blob = new Blob([text], { type: 'text/plain' });
	const url = window.URL.createObjectURL(blob);
	const link = document.createElement('a');
	link.href = url;
	link.setAttribute('download', 'template');
	document.body.appendChild(link);
	link.click();
	document.body.removeChild(link);
	window.URL.revokeObjectURL(url);
};

export const postGlossaryImportFormData = (
	file: File,
	onUploadProgress?: (progressPercent: number) => void
) => {
	const formData = new FormData();
	formData.append('file', file);
	return getGlossaryImport(formData, {
		onUploadProgress: (progressEvent: { loaded: number; total: number }) => {
			if (!onUploadProgress || !progressEvent.total) return;
			const progressValue =
				(progressEvent.loaded / progressEvent.total) * 100;
			onUploadProgress(progressValue);
		}
	});
};
