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

import { CSP_NONCE_META_NAME, getCspNonce } from "../cspNonce";

describe("getCspNonce", () => {
  const originalWindowNonce = window.__CSP_NONCE__;

  beforeEach(() => {
    document.head.innerHTML = "";
    document.body.innerHTML = "";
    delete window.__CSP_NONCE__;
  });

  afterEach(() => {
    if (originalWindowNonce === undefined) {
      delete window.__CSP_NONCE__;
    } else {
      window.__CSP_NONCE__ = originalWindowNonce;
    }
  });

  it("returns nonce from meta tag when present", () => {
    const meta = document.createElement("meta");
    meta.setAttribute("name", CSP_NONCE_META_NAME);
    meta.setAttribute("content", "meta-nonce-value");
    document.head.appendChild(meta);

    expect(getCspNonce()).toBe("meta-nonce-value");
  });

  it("trims whitespace from meta tag nonce", () => {
    const meta = document.createElement("meta");
    meta.setAttribute("name", CSP_NONCE_META_NAME);
    meta.setAttribute("content", "  trimmed-meta-nonce  ");
    document.head.appendChild(meta);

    expect(getCspNonce()).toBe("trimmed-meta-nonce");
  });

  it("prefers meta tag nonce over script nonce", () => {
    const meta = document.createElement("meta");
    meta.setAttribute("name", CSP_NONCE_META_NAME);
    meta.setAttribute("content", "meta-nonce-value");
    document.head.appendChild(meta);

    const script = document.createElement("script");
    script.setAttribute("nonce", "script-nonce-value");
    document.body.appendChild(script);

    expect(getCspNonce()).toBe("meta-nonce-value");
  });

  it("returns nonce from script tag when meta tag is missing", () => {
    const script = document.createElement("script");
    script.setAttribute("nonce", "script-nonce-value");
    document.body.appendChild(script);

    expect(getCspNonce()).toBe("script-nonce-value");
  });

  it("returns nonce from window global when dom sources are missing", () => {
    window.__CSP_NONCE__ = "window-nonce-value";

    expect(getCspNonce()).toBe("window-nonce-value");
  });

  it("returns undefined when no nonce source is available", () => {
    expect(getCspNonce()).toBeUndefined();
  });

  it("returns undefined when meta content is blank", () => {
    const meta = document.createElement("meta");
    meta.setAttribute("name", CSP_NONCE_META_NAME);
    meta.setAttribute("content", "   ");
    document.head.appendChild(meta);

    expect(getCspNonce()).toBeUndefined();
  });

  it("returns undefined when script nonce is blank and no other source exists", () => {
    const script = document.createElement("script");
    script.setAttribute("nonce", "   ");
    document.body.appendChild(script);

    expect(getCspNonce()).toBeUndefined();
  });

  it("falls back to script nonce when meta content is blank", () => {
    const meta = document.createElement("meta");
    meta.setAttribute("name", CSP_NONCE_META_NAME);
    meta.setAttribute("content", "   ");
    document.head.appendChild(meta);

    const script = document.createElement("script");
    script.setAttribute("nonce", "script-nonce-value");
    document.body.appendChild(script);

    expect(getCspNonce()).toBe("script-nonce-value");
  });
});
