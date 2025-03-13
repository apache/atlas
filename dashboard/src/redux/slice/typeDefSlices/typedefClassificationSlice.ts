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

export const fetchClassificationData = createAsyncThunk(
  "classification/fetchClassificationData",
  async (_, { rejectWithValue }) => {
    try {
      const response = await getTypeDef("classification");
      return response.data;
    } catch (error) {
      return rejectWithValue(error);
    }
  }
);

const initialState = {
  loadingClassification: false,
  classificationData: null,
  error: null as unknown
};

const classificationSlice = createSlice({
  name: "classification",
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchClassificationData.pending, (state) => {
        return {
          ...state,
          loadingClassification: true,
          error: null
        };
      })
      .addCase(fetchClassificationData.fulfilled, (state, action) => {
        return {
          ...state,
          loadingClassification: false,
          classificationData: action.payload
        };
      })
      .addCase(fetchClassificationData.rejected, (state, action) => {
        return {
          ...state,
          loadingClassification: false,
          error: action.payload
        };
      });
  }
});

export const classificationReducer = classificationSlice.reducer;
