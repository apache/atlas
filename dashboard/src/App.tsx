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

import { lazy, Suspense, useEffect } from "react";
import { CircularProgress } from "@mui/material";
import { useAppDispatch } from "@hooks/reducerHook";
import { fetchSessionData } from "@redux/slice/sessionSlice";

const Router = lazy(() => import("./views/Router"));

const App = () => {
  const dispatch = useAppDispatch();

  useEffect(() => {
    dispatch(fetchSessionData());
  }, []);

  return (
    <Suspense
      fallback={
        <>
          <div
            style={{
              position: "fixed",
              left: 0,
              top: 0,
              width: "100%",
              height: "100vh"
            }}
          >
            <CircularProgress
              disableShrink
              color="success"
              sx={{
                display: "inline-block",
                position: "absolute",
                left: "50%",
                top: "50%",
                transform: "translate(-50%, -50%)"
              }}
            />
          </div>
        </>
      }
    >
      <Router />
    </Suspense>
  );
};

export default App;
