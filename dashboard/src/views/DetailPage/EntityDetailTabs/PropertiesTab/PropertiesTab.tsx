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

import { Grid, Stack } from "@mui/material";
import AttributeProperties from "../AttributeProperties";
import Labels from "./Labels";
import UserDefinedProperties from "./UserDefinedProperties";
import BMAttributes from "./BMAttributes";

const PropertiesTab = (props: {
  entity: any;
  referredEntities: any;
  loading: boolean;
}) => {
  const { entity, referredEntities, loading } = props;
  const {
    customAttributes = {},
    labels = [],
    businessAttributes = {}
  } = entity || {};

  return (
    <Grid
      container
      marginTop={0}
      className="properties-container"
      sx={{ backgroundColor: "rgba(255,255,255,0.6)" }}
      flex="1"
    >
      <Grid item md={6} p={2} data-cy="technical-properties">
        <AttributeProperties
          entity={entity}
          referredEntities={referredEntities}
          loading={loading}
          propertiesName="Technical"
        />
      </Grid>
      <Grid item md={6} p={2}>
        <Stack gap={"1rem"}>
          <UserDefinedProperties
            loading={loading}
            customAttributes={customAttributes}
            entity={entity}
          />
          <Labels loading={loading} labels={labels} />

          <BMAttributes
            loading={loading}
            bmAttributes={businessAttributes}
            entity={entity}
          />
        </Stack>
      </Grid>
    </Grid>
  );
};

export default PropertiesTab;
