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

import { getRootClassificationDef } from "@api/apiMethods/classificationApiMethod";
import { createSlice, createAsyncThunk, PayloadAction } from "@reduxjs/toolkit";
import { addOnClassification } from "@utils/Enum";

type DynamicData = Record<any, any>;

interface TagState {
  rootClassificationTypeData: {
    loading: boolean;
    rootClassificationData: DynamicData | null;
    error: string | null;
  };
}

export const fetchRootClassification = createAsyncThunk<DynamicData, void>(
  "rootClassificationType/fetchRootClassification",
  async (): Promise<any> => {
    const response = await getRootClassificationDef(addOnClassification[0]);

    return response.data;
  }
) as any;

const enumInitialState: TagState = {
  rootClassificationTypeData: {
    loading: false,
    rootClassificationData: null,
    error: null
  }
};

const rootClassificationTypeDataSlice = createSlice({
  name: "rootClassificationType",
  initialState: enumInitialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(fetchRootClassification.pending, (state) => {
      state.rootClassificationTypeData = {
        loading: true,
        rootClassificationData: null,
        error: null
      };
    }),
      builder.addCase(
        fetchRootClassification.fulfilled,
        (state, action: PayloadAction<DynamicData>) => {
          state.rootClassificationTypeData = {
            loading: false,
            rootClassificationData: action.payload,
            error: null
          };
        }
      ),
      builder.addCase(fetchRootClassification.rejected, (state, action) => {
        state.rootClassificationTypeData = {
          loading: false,
          rootClassificationData: null,
          error: action.payload as string
        };
      });
  }
});

export const rootClassificationTypeReducer =
  rootClassificationTypeDataSlice.reducer;
