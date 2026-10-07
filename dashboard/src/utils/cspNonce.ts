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

export const CSP_NONCE_META_NAME = "csp-nonce";

declare global {
  interface Window {
    __CSP_NONCE__?: string;
  }
}

const getTrimmedAttribute = (value: string | null | undefined): string | undefined => {
  const trimmedValue = value?.trim();

  return trimmedValue ? trimmedValue : undefined;
};

export const getCspNonce = (): string | undefined => {
  const metaNonce = getTrimmedAttribute(
    document.querySelector(`meta[name="${CSP_NONCE_META_NAME}"]`)?.getAttribute("content")
  );

  if (metaNonce) {
    return metaNonce;
  }

  const scriptNonce = getTrimmedAttribute(
    document.querySelector("script[nonce]")?.getAttribute("nonce")
  );

  if (scriptNonce) {
    return scriptNonce;
  }

  return getTrimmedAttribute(window.__CSP_NONCE__);
};
