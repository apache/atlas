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

import { getVersion } from "@api/apiMethods/headerApiMethods";
import {
  CircularProgress,
  List,
  ListItem,
  ListItemText,
  Stack,
  Typography
} from "@mui/material";
import { serverError } from "@utils/Utils";
import { useEffect, useRef, useState } from "react";

const About = () => {
  const [versionData, setVersionData] = useState<any>({});
  const [loader, setLoader] = useState(false);
  const toastId = useRef(null);

  useEffect(() => {
    fetchVersionDetails();
  }, []);

  const fetchVersionDetails = async () => {
    setLoader(true);
    try {
      const versionResp = await getVersion();
      const { data = {} } = versionResp || {};
      setVersionData(data);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      console.error(`Error occur while fetching version details`, error);
      serverError(error, toastId);
    }
  };

  return (
    <>
      <Stack spacing={2}>
        {loader ? (
          <CircularProgress />
        ) : (
          <Stack direction="column" spacing={1}>
            <Typography variant="body1">
              <strong>Version: </strong>
              {versionData?.Version}
            </Typography>
            <Typography variant="body2" color="info.main">
              Get involved!
            </Typography>
            <Typography variant="body2" color="info.main">
              <List dense>
                <ListItem
                  button
                  component="a"
                  href="http://apache.org/licenses/LICENSE-2.0"
                  target="_blank"
                >
                  <ListItemText primary="Licensed under the Apache License Version 2.0" />
                </ListItem>
              </List>
            </Typography>
          </Stack>
        )}
      </Stack>
    </>
  );
};

export default About;
