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
import { getTypeDef } from "../../../api/apiMethods/typeDefApiMethods";
import { cloneDeep } from "@utils/Helper";

export const fetchBusinessMetaData = createAsyncThunk(
  "businessMetadata/fetchBusinessMetaData",
  async (_, { rejectWithValue }) => {
    try {
      const response = await getTypeDef("business_metadata");
      const payload = response.data;
      return cloneDeep(payload);
    } catch (error) {
      return rejectWithValue(error);
    }
  }
);

const initialState = {
  loading: false,
  businessMetaData: null,
  error: null as unknown
};

const businessMetadataSlice = createSlice({
  name: "businessMetadata",
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchBusinessMetaData.pending, (state) => {
        return {
          ...state,
          loading: true,
          error: null
        };
      })
      .addCase(fetchBusinessMetaData.fulfilled, (state, action) => {
        return {
          ...state,
          loading: false,
          businessMetaData: action.payload
        };
      })
      .addCase(fetchBusinessMetaData.rejected, (state, action) => {
        return {
          ...state,
          loading: false,
          error: action.payload
        };
      });
  }
});

export const businessMetadataReducer = businessMetadataSlice.reducer;
