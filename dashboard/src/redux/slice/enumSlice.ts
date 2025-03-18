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

import { getTypeDef } from "@api/apiMethods/typeDefApiMethods";
import { createSlice, createAsyncThunk, PayloadAction } from "@reduxjs/toolkit";

type DynamicData = Record<any, any>;

interface EnumState {
  enumObj: {
    loading: boolean;
    data: DynamicData | null;
    error: string | null;
  };
}

export const fetchEnumData = createAsyncThunk<DynamicData, void>(
  "enum/fetchEnumData",
  async () => {
    const response = await getTypeDef("enum");
    return response.data;
  }
);

const enumInitialState: EnumState = {
  enumObj: {
    loading: false,
    data: null,
    error: null
  }
};

const enumSlice = createSlice({
  name: "enum",
  initialState: enumInitialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(fetchEnumData.pending, (state) => {
      state.enumObj = {
        loading: true,
        data: null,
        error: null
      };
    }),
      builder.addCase(
        fetchEnumData.fulfilled,
        (state, action: PayloadAction<DynamicData>) => {
          state.enumObj = {
            loading: false,
            data: action.payload,
            error: null
          };
        }
      ),
      builder.addCase(fetchEnumData.rejected, (state, action) => {
        state.enumObj = {
          loading: false,
          data: null,
          error: action.payload as string
        };
      });
  }
});

export const enumReducer = enumSlice.reducer;
