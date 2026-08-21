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

const actualSanitizeModule = jest.requireActual('sanitize-html/index.js') as any;

const getSanitizeFn = () => {
    if (typeof actualSanitizeModule === 'function') return actualSanitizeModule;
    if (actualSanitizeModule && typeof actualSanitizeModule.default === 'function') return actualSanitizeModule.default;
    return null;
};

const sanitizeHtml = (html: string, _options?: Record<string, unknown>) => {
  const sanitizeFn = getSanitizeFn();
  
  if (_options && typeof sanitizeFn === 'function') {
    return sanitizeFn(html, _options);
  }
  
  const htmlStr = typeof html === 'string' ? html : String(html);
  return htmlStr.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '');
};
export default sanitizeHtml;
