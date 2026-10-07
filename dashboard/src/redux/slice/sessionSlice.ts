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

import { fetchApi } from "@api/apiMethods/fetchApi";
import { getSessionApiUrl } from "@api/apiUrlLinks/sessionApiUrl";
import { getVersion } from "@api/apiMethods/headerApiMethods";
import { createSlice, createAsyncThunk, PayloadAction } from "@reduxjs/toolkit";
import { globalSession } from "@utils/Global";

type DynamicData = Record<string, unknown>;

interface SessionState {
  sessionObj: {
    loading: boolean;
    data: DynamicData | null;
    error: string | null;
  };
  versionData: {
    loading: boolean;
    data: DynamicData | null;
    error: string | null;
  };
}

export const fetchSessionData = createAsyncThunk<DynamicData, void>(
  "session/fetchSessionData",
  async () => {
    const response = await fetchApi(getSessionApiUrl(), {
      method: "GET"
    });
    response.data && globalSession(response.data);
    return response.data;
  }
);

export const fetchVersionData = createAsyncThunk<DynamicData, void>(
  "session/fetchVersionData",
  async () => {
    const response = await getVersion();
    return response.data;
  }
);

const sessionInitialState: SessionState = {
  sessionObj: {
    loading: false,
    data: null,
    error: null
  },
  versionData: {
    loading: false,
    data: null as Record<string, unknown> | null,
    error: null
  }
};

const sessionSlice = createSlice({
  name: "session",
  initialState: sessionInitialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(fetchSessionData.pending, (state) => {
      state.sessionObj.loading = true;
      state.sessionObj.error = null;
    }),
      builder.addCase(
        fetchSessionData.fulfilled,
        (state, action: PayloadAction<DynamicData>) => {
          state.sessionObj = {
            loading: false,
            data: action.payload,
            error: null
          };
        }
      ),
      builder.addCase(fetchSessionData.rejected, (state, action) => {
        state.sessionObj = {
          loading: false,
          data: state.sessionObj.data,
          error: (action.payload as string) || action.error?.message || 'An error occurred'
        };
      }),
      builder.addCase(fetchVersionData.pending, (state) => {
        // Preserve existing state.versionData.data on pending (stale-while-revalidate)
        state.versionData.loading = true;
        state.versionData.error = null;
      }),
      builder.addCase(
        fetchVersionData.fulfilled,
        (state, action: PayloadAction<DynamicData>) => {
          state.versionData = {
            loading: false,
            data: action.payload,
            error: null
          };
        }
      ),
      builder.addCase(fetchVersionData.rejected, (state, action) => {
        state.versionData = {
          loading: false,
          data: state.versionData.data,
          error: (action.payload as string) || action.error?.message || 'An error occurred'
        };
      });
  }
});

export const sessionReducer = sessionSlice.reducer;
