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

jest.mock("@emotion/cache", () => ({
  __esModule: true,
  default: jest.fn(() => ({ key: "css" }))
}));

import createCache from "@emotion/cache";
import { createEmotionCache } from "../emotionCache";

describe("createEmotionCache", () => {
  const mockCreateCache = createCache as jest.MockedFunction<typeof createCache>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockCreateCache.mockReturnValue({ key: "css" });
  });

  it("creates emotion cache without nonce when nonce is undefined", () => {
    createEmotionCache(undefined);

    expect(mockCreateCache).toHaveBeenCalledWith({ key: "css" });
  });

  it("creates emotion cache without nonce when nonce is blank", () => {
    createEmotionCache("   ");

    expect(mockCreateCache).toHaveBeenCalledWith({ key: "css" });
  });

  it("creates emotion cache with nonce when nonce is provided", () => {
    createEmotionCache("server-generated-nonce");

    expect(mockCreateCache).toHaveBeenCalledWith({
      key: "css",
      nonce: "server-generated-nonce"
    });
  });

  it("returns the cache instance from @emotion/cache", () => {
    const cacheInstance = { key: "css", nonce: "server-generated-nonce" };
    mockCreateCache.mockReturnValue(cacheInstance);

    expect(createEmotionCache("server-generated-nonce")).toBe(cacheInstance);
  });
});
