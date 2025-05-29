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
 * limitations under the License.useForm
 */

import {
  InputLabel,
  Stack,
  TextField,
  ToggleButton,
  ToggleButtonGroup
} from "@mui/material";
import { useState } from "react";
import { Controller } from "react-hook-form";
import ReactQuill from "react-quill-new";

const GlossaryForm = (props: {
  control: any;
  handleSubmit: any;
  setValue: any;
}) => {
  const { control, handleSubmit, setValue } = props;
  const [alignment, setAlignment] = useState<string>("formatted");

  const handleChange = (
    event: React.MouseEvent<HTMLElement>,
    newAlignment: string
  ) => {
    event?.stopPropagation();
    if (newAlignment != null) {
      setAlignment(newAlignment);
    }
  };
  return (
    <>
      <Stack>
        <form onSubmit={handleSubmit}>
          <Stack marginBottom="2.5rem">
            <Controller
              control={control}
              name={"name"}
              rules={{
                required: true
              }}
              render={({
                field: { onChange, value },
                fieldState: { error }
              }) => (
                <>
                  <InputLabel required>Name</InputLabel>

                  <TextField
                    margin="normal"
                    error={!!error}
                    fullWidth
                    value={value}
                    onChange={(e) => {
                      const value = e.target.value;
                      onChange(value);
                    }}
                    variant="outlined"
                    size="small"
                    placeholder={"Name required"}
                    // helperText={error ? "This field is required" : ""}
                    className="form-textfield"
                  />
                </>
              )}
            />
            <Controller
              control={control}
              name={"shortDescription"}
              render={({ field: { onChange, value } }) => (
                <>
                  <InputLabel>Short Description</InputLabel>

                  <TextField
                    margin="normal"
                    fullWidth
                    value={value}
                    onChange={(e) => {
                      const value = e.target.value;
                      onChange(value);
                    }}
                    variant="outlined"
                    size="small"
                    className="form-textfield"
                  />
                </>
              )}
            />
            <Controller
              control={control}
              name={"longDescription"}
              render={({ field }) => (
                <Stack gap="0.5rem">
                  <Stack
                    direction="row"
                    justifyContent="space-between"
                    alignItems="center"
                  >
                    <InputLabel>Long Description</InputLabel>
                    <ToggleButtonGroup
                      size="small"
                      color="primary"
                      value={alignment}
                      exclusive
                      onChange={(e, newValue) => {
                        e.stopPropagation();
                        handleChange(e, newValue);
                      }}
                      aria-label="Platform"
                    >
                      <ToggleButton
                        className="entity-form-toggle-btn"
                        value="formatted"
                        data-cy="formatted"
                      >
                        Formatted Text
                      </ToggleButton>
                      <ToggleButton
                        value="plain"
                        className="entity-form-toggle-btn"
                        data-cy="plain"
                      >
                        Plain text
                      </ToggleButton>
                    </ToggleButtonGroup>
                  </Stack>
                  {alignment == "formatted" ? (
                    <div style={{ position: "relative" }}>
                      <ReactQuill
                        {...field}
                        theme="snow"
                        placeholder={"Description required"}
                        onChange={(text) => {
                          field.onChange(text);
                          setValue("description", text);
                        }}
                        className="classification-form-editor"
                      />
                    </div>
                  ) : (
                    <textarea
                      {...field}
                      className="form-textarea-field"
                      placeholder={"Long Description"}
                      onChange={(e) => {
                        e.stopPropagation();
                        const value = e.target.value;
                        field.onChange(value);
                        setValue("description", value);
                      }}
                      style={{ width: "100%" }}
                    />
                  )}
                </Stack>
              )}
            />
          </Stack>
        </form>
      </Stack>
    </>
  );
};

export default GlossaryForm;
