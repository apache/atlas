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

import Stepper from "@mui/material/Stepper";
import Step from "@mui/material/Step";
import StepButton from "@mui/material/StepButton";
import Stack from "@mui/material/Stack";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import FormTreeView from "@components/Forms/FormTreeView";
import { TextField, InputLabel } from "@mui/material";
import { Controller } from "react-hook-form";

const steps = ["Select Term", "Attributes"];

const TreeSearchInput = ({
  value,
  onChange
}: {
  value: string;
  onChange: (v: string) => void;
}) => (
  <TextField
    sx={{ position: "sticky", top: "0", background: "white" }}
    id="outlined-search"
    fullWidth
    label="Search Term"
    type="Search Term"
    size="small"
    value={value}
    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
      onChange(e.target.value)
    }
  />
);

const FormInputField = ({
  control,
  name,
  label,
  placeholder
}: {
  control: any;
  name: string;
  label: string;
  placeholder: string;
}) => (
  <Stack direction="row" gap="2rem">
    <Controller
      control={control}
      name={name}
      render={({ field: { onChange, value } }) => (
        <>
          <div className="form-fields" style={{ textAlign: "right" }}>
            <InputLabel>{label}</InputLabel>
          </div>
          <TextField
            margin="normal"
            fullWidth
            value={value}
            onChange={(e) => onChange(e.target.value)}
            variant="outlined"
            size="small"
            placeholder={placeholder}
            className="form-textfield"
          />
        </>
      )}
    />
  </Stack>
);

const StepperContent = ({
  activeStep,
  allStepsCompleted,
  handleReset,
  searchTerm,
  setSearchTerm,
  treeData,
  loader,
  handleNodeSelect,
  control,
  handleSubmit,
  onSubmit
}: any) => (
  <Stack
    gap="1rem"
    sx={{
      background: "#f5f5f5",
      overflowY: "auto",
      maxHeight: "500px"
    }}
    position="relative"
    padding={"1rem"}
  >
    {allStepsCompleted() ? (
      <>
        <Typography sx={{ mt: 2, mb: 1 }}>
          All steps completed - you&apos;re finished
        </Typography>
        <Stack sx={{ display: "flex", flexDirection: "row", pt: 2 }}>
          <Stack sx={{ flex: "1 1 auto" }} />
          <Button onClick={handleReset}>Reset</Button>
        </Stack>
      </>
    ) : (
      <>
        {activeStep === 0 ? (
          <>
            <TreeSearchInput value={searchTerm} onChange={setSearchTerm} />
            <FormTreeView
              treeData={treeData}
              searchTerm={searchTerm}
              treeName={"Term"}
              loader={loader}
              onNodeSelect={handleNodeSelect}
            />
          </>
        ) : (
          <Stack>
            <form onSubmit={handleSubmit(onSubmit)}>
              <Stack direction="row" gap="2rem">
                <FormInputField
                  control={control}
                  name="description"
                  label="description"
                  placeholder="description"
                />
              </Stack>
              <Stack direction="row" gap="2rem">
                <FormInputField
                  control={control}
                  name="expression"
                  label="expression"
                  placeholder="expression"
                />
              </Stack>
              <Stack direction="row" gap="2rem">
                <FormInputField
                  control={control}
                  name="steward"
                  label="steward"
                  placeholder="steward"
                />
              </Stack>
              <Stack direction="row" gap="2rem">
                <FormInputField
                  control={control}
                  name="source"
                  label="source"
                  placeholder="source"
                />
              </Stack>
            </form>
          </Stack>
        )}
      </>
    )}
  </Stack>
);

const AssignRelatedTerm = ({
  activeStep,
  completed,
  handleStep,
  handleBack,
  handleNext,
  handleReset,
  allStepsCompleted,
  searchTerm,
  setSearchTerm,
  treeData,
  loader,
  handleNodeSelect,
  control,
  handleSubmit,
  onSubmit
}: any) => (
  <Stack gap="16px" sx={{ width: "100%" }}>
    <Stepper nonLinear activeStep={activeStep} sx={{ gap: "0.5rem" }}>
      {steps.map((label, index) => (
        <Step
          sx={{ padding: 0, display: "flex", justifyContent: "center" }}
          key={label}
          completed={completed[index]}
        >
          <StepButton
            sx={{
              height: "72px",
              paddingLeft: "8px",
              paddingRight: "8px"
            }}
            color="inherit"
            className="m-0 py-0"
            onClick={handleStep(index)}
          >
            {label}
          </StepButton>
        </Step>
      ))}
    </Stepper>
    <StepperContent
      activeStep={activeStep}
      allStepsCompleted={allStepsCompleted}
      handleReset={handleReset}
      searchTerm={searchTerm}
      setSearchTerm={setSearchTerm}
      treeData={treeData}
      loader={loader}
      handleNodeSelect={handleNodeSelect}
      control={control}
      handleSubmit={handleSubmit}
      onSubmit={onSubmit}
    />
    <Stack sx={{ display: "flex", flexDirection: "row", pt: 2 }}>
      <Button
        color="inherit"
        disabled={activeStep === 0}
        onClick={handleBack}
        sx={{ mr: 1 }}
      >
        Back
      </Button>
      <Stack sx={{ flex: "1 1 auto" }} />
      <Button
        disabled={activeStep === steps.length - 1}
        onClick={handleNext}
        sx={{ mr: 1 }}
      >
        Next
      </Button>
    </Stack>
  </Stack>
);

export default AssignRelatedTerm;
