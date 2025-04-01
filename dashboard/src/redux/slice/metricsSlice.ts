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

import { createSlice, createAsyncThunk } from "@reduxjs/toolkit";
import { getMetricsEntity } from "../../api/apiMethods/metricsApiMethods";

export const fetchMetricEntity = createAsyncThunk(
  "metrics/fetchMetricEntity",
  async (_, { rejectWithValue }) => {
    try {
      const response: any = await getMetricsEntity({});
      return response.data;
    } catch (error) {
      console.error("Error fetching metrics data:", error);
      return rejectWithValue(error);
    }
  }
);

const metricsSlice = createSlice({
  name: "metrics",
  initialState: {
    loading: true,
    metricsData: null,
    error: null as unknown
  },
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchMetricEntity.pending, (state) => {
        state.loading = true;
      })
      .addCase(fetchMetricEntity.fulfilled, (state, action) => {
        state.loading = false;
        state.metricsData = action.payload;
      })
      .addCase(fetchMetricEntity.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload;
      });
  }
});

export const metricsReducer = metricsSlice.reducer;
