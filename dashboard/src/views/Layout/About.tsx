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

import { useAppSelector } from "@hooks/reducerHook";
import SkeletonLoader from "@components/SkeletonLoader";
import {
  List,
  ListItem,
  ListItemText,
  Stack,
  Typography
} from "@mui/material";

const About = () => {
  const { data: versionData, loading: loader } = useAppSelector((state: any) => state.session.versionData);

  return (
    <>
      <Stack spacing={2}>
        {loader ? (
          <SkeletonLoader animation="wave" variant="text" width={'100%'} count={3} sx={{marginTop: '0px !important'}}/>
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
