/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Select from "react-dropdown-select";
import React, { Fragment, useState } from "react";
import styled from "styled-components";
import * as colors from "../../../../styles/colors";

const Basic = props => {
  let { options, ...rest } = props;
  const [value, setValue] = useState([
    { id: 1, href: "/", title: "Latest", label: "Latest" }
  ]);

  const ComponentStyle = styled.div`
    > div {
      display: inline-block;
      width: 145px;
    }
    label {
      margin-right: 5px;
    }
  `;

  return (
    <ComponentStyle>
      <Fragment>
        <label>Versions:</label>
        <Select
          valueField="id"
          placeholder="Documentation"
          color={colors.green}
          options={options}
          values={value}
          onChange={selectedValue => {
            if (selectedValue.length > 0 && value !== selectedValue) {
              let href = selectedValue[0].href,
                target = "_self";
              setValue(selectedValue);
              if (selectedValue[0].title !== "Latest") {
                href = `http://atlas.apache.org${selectedValue[0].href}`;
                target = "_blank";
              }
              window.open(href, target);
            }
          }}
          {...rest}
        />
      </Fragment>
    </ComponentStyle>
  );
};
Basic.propTypes = {};
export default Basic;