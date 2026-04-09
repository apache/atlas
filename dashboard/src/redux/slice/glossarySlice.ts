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
import { getGlossary } from "../../api/apiMethods/glossaryApiMethod";

export const fetchGlossaryData = createAsyncThunk(
  "glossary/fetchGlossaryData",
  async (_, { rejectWithValue }) => {
    try {
      const response: any = await getGlossary();
      return response.data;
    } catch (error) {
      console.error("Error fetching glossary data:", error);
      return rejectWithValue(error);
    }
  }
);

const glossarySlice = createSlice({
  name: "glossary",
  initialState: {
    loading: true,
    glossaryData: null,
    error: null as unknown
  },
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchGlossaryData.pending, (state) => {
        state.loading = true;
      })
      .addCase(fetchGlossaryData.fulfilled, (state, action) => {
        state.loading = false;
        state.glossaryData = action.payload;
      })
      .addCase(fetchGlossaryData.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload;
      });
  }
});

export const glossaryReducer = glossarySlice.reducer;
