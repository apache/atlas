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

import { getGlossaryType } from "@api/apiMethods/glossaryApiMethod";
import { createSlice, createAsyncThunk, PayloadAction } from "@reduxjs/toolkit";

type DynamicData = Record<any, any>;

interface EnumState {
  glossaryTypeData: {
    loading: boolean;
    data: DynamicData | null;
    error: string | null;
  };
}

export const fetchGlossaryDetails = createAsyncThunk<DynamicData, void>(
  "glossaryType/fetchGlossaryDetails",
  async (params: any): Promise<any> => {
    const { gtype, guid }: any = params;
    const response = await getGlossaryType(
      gtype as unknown as string,
      guid as unknown as string
    );

    return response.data;
  }
) as any;

const enumInitialState: EnumState = {
  glossaryTypeData: {
    loading: false,
    data: null,
    error: null
  }
};

const glossaryTypeSlice = createSlice({
  name: "glossaryType",
  initialState: enumInitialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(fetchGlossaryDetails.pending, (state) => {
      state.glossaryTypeData = {
        loading: true,
        data: null,
        error: null
      };
    }),
      builder.addCase(
        fetchGlossaryDetails.fulfilled,
        (state, action: PayloadAction<DynamicData>) => {
          state.glossaryTypeData = {
            loading: false,
            data: action.payload,
            error: null
          };
        }
      ),
      builder.addCase(fetchGlossaryDetails.rejected, (state, action) => {
        state.glossaryTypeData = {
          loading: false,
          data: null,
          error: action.payload as string
        };
      });
  }
});

export const glossaryTypeReducer = glossaryTypeSlice.reducer;
