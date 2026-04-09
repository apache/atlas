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
import { ThemeProvider, createTheme } from "@mui/material/styles";
import * as React from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.scss";
import { ToastContainer } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import "./styles/font-awesome.min.css";
import "react-datepicker/dist/react-datepicker.css";
import "react-querybuilder/dist/query-builder.scss";
import "react-quill-new/dist/quill.snow.css";
import "react-quill-new/dist/quill.bubble.css";
import "react-quill-new/dist/quill.core.css";
import "../src/styles/table.scss";
import { Provider } from "react-redux";
import store from "./redux/store/store.ts";
// import ErrorBoundary from "ErrorBoundary.ts";

const theme = createTheme({
  typography: {
    allVariants: {
      fontFamily: "'Source Sans 3', sans-serif",
      textTransform: "none",
      fontSize: 14
    }
  }
});

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <Provider store={store}>
        <App />
      </Provider>
      <ToastContainer />
    </ThemeProvider>
  </React.StrictMode>
);
