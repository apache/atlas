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

import { createSlice, PayloadAction } from "@reduxjs/toolkit";

interface DrawerState {
  isOpen: boolean;
  activeId: string | null;
}

const initialState: DrawerState = {
  isOpen: false,
  activeId: null
};

const drawerSlice = createSlice({
  name: "drawer",
  initialState,
  reducers: {
    openDrawer: (state, action: PayloadAction<string>) => {
      state.isOpen = true;
      state.activeId = action.payload;
    },
    closeDrawer: (state) => {
      state.isOpen = false;
      state.activeId = null;
    },
    toggleDrawer: (state) => {
      state.isOpen = !state.isOpen;
      if (!state.isOpen) {
        state.activeId = null;
      }
    }
  }
});

export const { openDrawer, closeDrawer, toggleDrawer } = drawerSlice.actions;
export const drawerSliceReducer = drawerSlice.reducer;
