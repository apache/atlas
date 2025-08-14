// @ts-nocheck

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

import { CustomButton, LightTooltip } from "@components/muiComponents";
import {
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
  Typography,
  Grid,
  List,
  ListItem,
  ListItemText,
  FormControlLabel,
  Checkbox,
  Autocomplete,
  Tooltip,
  tooltipClasses,
  TooltipProps,
  styled,
  FilterOptionsState
} from "@mui/material";
import { customSortBy, isEmpty, serverError } from "@utils/Utils";
import { Controller, useForm } from "react-hook-form";
import ClearOutlinedIcon from "@mui/icons-material/ClearOutlined";
import HelpOutlinedIcon from "@mui/icons-material/HelpOutlined";
import ArrowUpwardOutlinedIcon from "@mui/icons-material/ArrowUpwardOutlined";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import { dataTypes, searchWeight } from "@utils/Enum";
import { Key, useRef, useState } from "react";
import CustomModal from "@components/Modal";
import EnumCreateUpdate from "./EnumCreateUpdate";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { createEnum, updateEnum } from "@api/apiMethods/typeDefApiMethods";
import { fetchEnumData } from "@redux/slice/enumSlice";
import { toast } from "react-toastify";

const HtmlTooltip = styled(({ className, ...props }: TooltipProps) => (
  <Tooltip
    {...props}
    classes={{ popper: className }}
    arrow
    placement="bottom-start"
  />
))(({ theme }) => ({
  [`& .${tooltipClasses.tooltip}`]: {
    backgroundColor: "#f5f5f9",
    color: "rgba(0, 0, 0, 0.87)",
    maxWidth: 500,
    fontSize: theme.typography.pxToRem(12),
    border: "1px solid #dadde9"
  }
}));

const BusinessMetadataAttributeForm = ({
  fields,
  control,
  remove,
  watched,
  dataTypeOptions,
  enumTypes,
  watch: attributeDefsWatch,
  setValue: attributeDefsSetValue
}: any) => {
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { enumDefs } = enumObj?.data || {};
  const { editbmAttribute }: any = useAppSelector((state) => state.createBM);
  const [enumModal, setEnumModal] = useState<boolean>(false);
  const dispatchState = useAppDispatch();
  const {
    control: enumControl,
    handleSubmit,
    watch,
    setValue,
    reset,
    formState: { isSubmitting }
  } = useForm();
  const toastId: any = useRef(null);

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    let isPutCall = false;
    let isPostCallEnum = false;
    const { enumType = "", enumValues = [] } = formData || {};

    const selectedEnumValues = !isEmpty(enumValues)
      ? enumValues.map((enumVal: { value: any }) => enumVal.value)
      : [];
    let newEnumDef = [];

    const enumName = !isEmpty(enumDefs)
      ? enumDefs?.find((enumDef: { name: string }) => enumDef.name === enumType)
      : {};
    const { elementDefs = [] } = enumName || {};
    if (!isEmpty(enumName)) {
      let enumDef = elementDefs || [];
      if (enumDef.length === selectedEnumValues.length) {
        enumDef.forEach((enumVal: { value: any }) => {
          if (!selectedEnumValues.includes(enumVal.value)) {
            isPutCall = true;
          }
        });
      } else {
        isPutCall = true;
      }
    } else {
      isPostCallEnum = true;
    }
    let elementValues: { ordinal: number; value: any }[] = [];
    selectedEnumValues?.forEach((inputEnumVal: any, index: number) => {
      elementValues?.push({
        ordinal: index + 1,
        value: inputEnumVal
      });
    });

    newEnumDef?.push({
      name: enumType,
      elementDefs: elementValues
    });
    let data = {
      enumDefs: newEnumDef
    };

    try {
      if (isPostCallEnum) {
        await createEnum(data);
        toast.dismiss(toastId.current);
        toastId.current = toast.success(
          `Enumeration ${enumType} 
               added
                 successfully`
        );
      } else if (isPutCall) {
        await updateEnum(data);
        toast.dismiss(toastId.current);
        toastId.current = toast.success(
          `Enumeration ${enumType} updated
                 successfully`
        );
      } else {
        toast.dismiss(toastId.current);
        toastId.current = toast.success("No updated values");
      }
      handleCloseEnumModal();
      reset({ enumType: "", enumValues: [] });
      dispatchState(fetchEnumData());
    } catch (error) {
      console.log(`Error occur while creating or updating Enum`, error);
      serverError(error, toastId);
    }
  };
  const handleCloseEnumModal = () => {
    setEnumModal(false);
  };

  const filterOptions = (
    options: any[],
    { inputValue }: FilterOptionsState<any>,
    selectedValues: { value: string }[]
  ) => {
    const lowerInputValue = inputValue ? inputValue.toLowerCase() : "";
    const filteredOptions: any[] = [];

    let selectedEnumValues = !isEmpty(selectedValues)
      ? selectedValues.map((obj: { value: string }) => {
          return obj.value.toLowerCase();
        })
      : [];

    options.forEach((option: { value: string }) => {
      const labelLower = option.value.toLowerCase();
      if (
        labelLower.includes(lowerInputValue) &&
        !selectedEnumValues.includes(labelLower)
      ) {
        filteredOptions.push(option);
      }
    });

    return filteredOptions;
  };

  return fields.map(
    (
      field: {
        id: Key | null | undefined;
        typeName: any;
        searchWeight: any;
        options: { maxStrLength: any };
      },
      index: string | number
    ): any => {
      const typeFieldName = `attributeDefs.${index}.enumType`;
      const selectedEnum = attributeDefsWatch(typeFieldName as any);

      let selectedEnumObj = !isEmpty(enumDefs)
        ? enumDefs.find((obj: { name: any }) => {
            return obj.name == selectedEnum;
          })
        : {};
      let selectedEnumValues = !isEmpty(selectedEnumObj)
        ? selectedEnumObj?.elementDefs
        : [];

      let enumTypeOptions = [...selectedEnumValues];
      return (
        <>
          <fieldset
            className="entity-form-fieldset"
            style={{ position: "relative", padding: "16px" }}
            key={field?.id}
          >
            {isEmpty(editbmAttribute) && (
              <div
                style={{
                  display: "flex",
                  justifyContent: "flex-end",
                  right: "8px",
                  top: "8px",
                  position: "absolute"
                }}
              >
                <IconButton
                  aria-label="back"
                  color="error"
                  size="small"
                  onClick={(_e) => {
                    remove(index);
                  }}
                >
                  <ClearOutlinedIcon />
                </IconButton>
              </div>
            )}
            <Controller
              control={control}
              name={`attributeDefs.${index}.name` as const}
              data-cy={`attributeDefs.${index}.name`}
              key={`attributeDefs.${index}.name`}
              defaultValue={field?.name}
              render={({ field: { value, onChange } }) => (
                <Grid
                  container
                  columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                  marginBottom="1rem"
                  marginTop="1rem"
                  alignItems="center"
                >
                  <Grid item md={3} textAlign="right">
                    <InputLabel required>Name</InputLabel>
                  </Grid>
                  <Grid item md={7}>
                    <TextField
                      disabled={isEmpty(editbmAttribute) ? false : true}
                      value={value}
                      sx={{
                        "& .MuiInputBase-root.Mui-disabled": {
                          backgroundColor: "#eee",
                          cursor: "not-allowed"
                        }
                      }}
                      onChange={onChange}
                      margin="none"
                      fullWidth
                      variant="outlined"
                      size="small"
                      placeholder={"Attribute Name"}
                    />
                  </Grid>
                </Grid>
              )}
            />

            <Controller
              control={control}
              name={`attributeDefs.${index}.typeName` as const}
              data-cy={`attributeDefs.${index}.typeName`}
              key={`attributeDefs.${index}.typeName`}
              defaultValue={field?.typeName}
              render={({ field: { value, onChange } }) => (
                <>
                  <Grid
                    container
                    columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                    marginBottom="1rem"
                    alignItems="center"
                  >
                    <Grid item md={3} textAlign="right">
                      <InputLabel required>Type</InputLabel>
                    </Grid>
                    <Grid item md={7}>
                      <div style={{ width: "100%" }}>
                        <Select
                          sx={{
                            "&.Mui-disabled": {
                              backgroundColor: "#eee",
                              cursor: "not-allowed"
                            }
                          }}
                          disabled={isEmpty(editbmAttribute) ? false : true}
                          value={value}
                          onChange={(event) => {
                            onChange(event.target.value);
                            if (event.target.value != "enumeration") {
                              attributeDefsSetValue(
                                `attributeDefs.${index}.enumType`,
                                ""
                              ) &&
                                attributeDefsSetValue(
                                  `attributeDefs.${index}.enumValues`,
                                  []
                                );
                            }
                          }}
                          fullWidth
                          size="small"
                          id="demo-select-small"
                        >
                          {dataTypes.map((type) => (
                            <MenuItem key={type} value={type}>
                              {type}
                            </MenuItem>
                          ))}
                        </Select>
                      </div>
                    </Grid>
                  </Grid>
                </>
              )}
            />

            <Controller
              control={control}
              name={`attributeDefs.${index}.searchWeight` as const}
              data-cy={`attributeDefs.${index}.searchWeight`}
              key={`attributeDefs.${index}.searchWeight` as const}
              defaultValue={field?.searchWeight}
              render={({ field: { value, onChange } }) => (
                <>
                  <Grid
                    container
                    columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                    marginBottom="1rem"
                    alignItems="center"
                  >
                    <Grid item md={3} textAlign="right">
                      <InputLabel required>Search Weight</InputLabel>
                    </Grid>
                    <Grid item md={7}>
                      <Stack
                        gap={1}
                        direction="row"
                        alignItems="center"
                        position="relative"
                      >
                        <div style={{ width: "100%" }}>
                          <Select
                            fullWidth
                            value={value}
                            onChange={onChange}
                            size="small"
                            defaultValue="string"
                            id="demo-select-small"
                          >
                            {searchWeight.map((type) => (
                              <MenuItem key={type} value={type}>
                                {type}
                              </MenuItem>
                            ))}
                          </Select>
                        </div>
                        <HtmlTooltip
                          arrow
                          title={
                            <>
                              <Typography color="inherit">
                                <ArrowUpwardOutlinedIcon fontSize="small" />
                                the search weight for the attribute,
                                <ArrowUpwardOutlinedIcon fontSize="small" />
                                the entity in the topmost search results when
                                searched for by that attribute
                              </Typography>
                              <br />
                              <Typography
                                sx={{
                                  textDecoration: "underline"
                                }}
                              >
                                Applicable changes
                              </Typography>

                              <Grid container>
                                <Grid>
                                  <List className="advanced-search-queries-list">
                                    <ListItem className="advanced-search-queries-listitem">
                                      <ListItemText
                                        primary={"Quick Search : 0-10"}
                                      />
                                      <ListItemText
                                        primary={"Suggestion : 8-10"}
                                      />
                                    </ListItem>
                                  </List>
                                </Grid>
                              </Grid>
                            </>
                          }
                        >
                          <HelpOutlinedIcon
                            sx={{ position: "absolute", right: "-28px" }}
                            fontSize="small"
                          />
                        </HtmlTooltip>
                      </Stack>
                    </Grid>{" "}
                  </Grid>
                </>
              )}
            />

            {((!isEmpty(editbmAttribute) &&
              watched?.[index] &&
              watched?.[index]?.multiValueSelect) ||
              isEmpty(editbmAttribute)) && (
              <Controller
                control={control}
                name={`attributeDefs.${index}.multiValueSelect` as const}
                key={`attributeDefs.${index}.multiValueSelect`}
                data-cy={`attributeDefs.${index}.multiValueSelect`}
                defaultValue={field?.multiValueSelect}
                render={({ field: { value, onChange } }) => (
                  <>
                    <Grid
                      container
                      columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                      marginBottom="1rem"
                      alignItems="center"
                    >
                      <Grid item md={3} textAlign="right">
                        <InputLabel>Enable Multivalues</InputLabel>
                      </Grid>
                      <Grid item md={7}>
                        {" "}
                        <FormControlLabel
                          control={
                            <Checkbox
                              disabled={isEmpty(editbmAttribute) ? false : true}
                              size="small"
                              checked={value}
                              onChange={onChange}
                            />
                          }
                          label={undefined}
                        />
                      </Grid>{" "}
                    </Grid>
                  </>
                )}
              />
            )}
            {watched?.[index] && watched?.[index]?.typeName == "string" && (
              <Controller
                control={control}
                name={`attributeDefs.${index}.options.maxStrLength` as const}
                data-cy={`attributeDefs.${index}.options.maxStrLength`}
                key={`attributeDefs.${index}.options.maxStrLength`}
                defaultValue={field?.options?.maxStrLength}
                render={({ field: { value, onChange } }) => (
                  <>
                    <Grid
                      container
                      columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                      marginBottom="1rem"
                      alignItems="center"
                    >
                      <Grid item md={3} textAlign="right">
                        {" "}
                        <InputLabel required>Max Length</InputLabel>
                      </Grid>
                      <Grid item md={7}>
                        <Stack
                          gap={1}
                          direction="row"
                          alignItems="center"
                          position="relative"
                        >
                          <TextField
                            value={value}
                            onChange={onChange}
                            data-cy="stringLength"
                            margin="none"
                            fullWidth
                            variant="outlined"
                            size="small"
                            type="number"
                            placeholder={"Maximum length"}
                          />
                          <HtmlTooltip
                            arrow
                            title={
                              <>
                                <Typography fontSize="14" color="inherit">
                                  String length limit includes any HTML
                                  formatting tags used.
                                </Typography>
                              </>
                            }
                          >
                            <HelpOutlinedIcon
                              sx={{ position: "absolute", right: "-28px" }}
                              fontSize="small"
                            />
                          </HtmlTooltip>
                        </Stack>
                      </Grid>{" "}
                    </Grid>
                  </>
                )}
              />
            )}

            {watched?.[index] &&
              watched?.[index]?.typeName == "enumeration" && (
                <Controller
                  name={`attributeDefs[${index}].enumType` as const}
                  control={control}
                  data-cy={`attributeDefs.${index}.enumType`}
                  key={`attributeDefs.${index}.enumType`}
                  defaultValue={field?.enumType}
                  render={({ field: { value, onChange } }) => (
                    <>
                      <Grid
                        container
                        columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                        marginBottom="1rem"
                        alignItems="center"
                      >
                        <Grid item md={3} textAlign="right">
                          <InputLabel required>Enum Name</InputLabel>
                        </Grid>
                        <Grid item md={7}>
                          <Stack direction="row" gap={1}>
                            <Autocomplete
                              readOnly={isEmpty(editbmAttribute) ? false : true}
                              sx={{
                                flex: "1",
                                "& .MuiInputBase-root.Mui-disabled": {
                                  backgroundColor: "#eee",
                                  cursor: "not-allowed"
                                }
                              }}
                              size="small"
                              id="search-by-type"
                              value={value || []}
                              clearIcon={null}
                              onChange={(_event, newValue) => {
                                onChange(newValue);

                                if (newValue) {
                                  let selectedEnumObj = enumDefs.find(
                                    (obj: { name: any }) => {
                                      return obj.name == newValue;
                                    }
                                  );
                                  let selectedEnumValues = !isEmpty(
                                    selectedEnumObj
                                  )
                                    ? selectedEnumObj?.elementDefs
                                    : [];

                                  let enumTypeOptions = [...selectedEnumValues];
                                  attributeDefsSetValue(
                                    `attributeDefs.${index}.enumValues`,
                                    enumTypeOptions
                                  );
                                }
                              }}
                              filterSelectedOptions
                              isOptionEqualToValue={(option, value) =>
                                option === value
                              }
                              options={
                                !isEmpty(enumTypes)
                                  ? enumTypes.map((option: any) => option)
                                  : []
                              }
                              className="advanced-search-autocomplete"
                              renderInput={(params) => (
                                <TextField
                                  {...params}
                                  fullWidth
                                  disabled={
                                    isEmpty(editbmAttribute) ? false : true
                                  }
                                  // label="Select type"
                                  InputLabelProps={{
                                    style: {
                                      top: "unset",
                                      bottom: "16px"
                                    }
                                  }}
                                  InputProps={{
                                    style: {
                                      padding: "0px 32px 0px 4px",
                                      height: "36px",
                                      lineHeight: "1.2"
                                    },
                                    ...params.InputProps,
                                    type: "search"
                                  }}
                                />
                              )}
                            />
                            {isEmpty(editbmAttribute) && (
                              <LightTooltip title={"Create/Update Enum"}>
                                <CustomButton
                                  variant="outlined"
                                  color="success"
                                  className="table-filter-btn assignTag"
                                  size="small"
                                  data-cy="createNewEnum"
                                  onClick={() => {
                                    setEnumModal(true);
                                  }}
                                >
                                  <EditOutlinedIcon className="table-filter-refresh" />{" "}
                                  Enum
                                </CustomButton>
                              </LightTooltip>
                            )}
                          </Stack>
                        </Grid>{" "}
                      </Grid>
                    </>
                  )}
                />
              )}

            {watched?.[index] && !isEmpty(watched?.[index]?.enumType) && (
              <Controller
                control={control}
                name={`attributeDefs[${index}].enumValues` as const}
                data-cy={`attributeDefs.${index}.enumValues`}
                key={`attributeDefs.${index}.enumValues` as const}
                defaultValue={field?.enumValues}
                rules={{
                  required: true
                }}
                render={({ field }) => {
                  return (
                    <>
                      <Grid
                        container
                        columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                        marginBottom="1rem"
                        alignItems="center"
                      >
                        <Grid item md={3} textAlign="right">
                          <InputLabel required>Enum Value</InputLabel>
                        </Grid>
                        <Grid item md={7}>
                          <Autocomplete
                            size="small"
                            readOnly
                            disableClearable={true}
                            multiple={true}
                            value={watched?.[index]?.enumValues || []}
                            getOptionLabel={(option) => option.value}
                            data-cy="enumValueSelector"
                            options={
                              !isEmpty(enumTypeOptions)
                                ? customSortBy(enumTypeOptions, ["value"])
                                : []
                            }
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                fullWidth
                                disabled
                                sx={{
                                  "& .MuiInputBase-root.Mui-disabled": {
                                    backgroundColor: "#eee",
                                    cursor: "not-allowed"
                                  }
                                }}
                                InputLabelProps={{
                                  style: {
                                    top: "unset",
                                    bottom: "16px"
                                  }
                                }}
                                InputProps={{
                                  ...params.InputProps,
                                  type: "search"
                                }}
                              />
                            )}
                          />
                        </Grid>{" "}
                      </Grid>
                    </>
                  );
                }}
              />
            )}
            <Controller
              control={control}
              name={
                `attributeDefs.${index}.options.applicableEntityTypes` as const
              }
              data-cy={`attributeDefs.${index}.options.applicableEntityTypes`}
              defaultValue={field?.options?.applicableEntityTypes}
              key={
                `attributeDefs.${index}.options.applicableEntityTypes` as const
              }
              render={({ field: { value, onChange } }) => {
                return (
                  <>
                    <Grid
                      container
                      columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                      marginBottom="1rem"
                      alignItems="center"
                    >
                      <Grid item md={3} textAlign="right">
                        <InputLabel>Applicable Types</InputLabel>
                      </Grid>
                      <Grid item md={7}>
                        <Autocomplete
                          multiple={true}
                          value={!isEmpty(value) ? value : []}
                          size="small"
                          disableClearable
                          id="search-by-type"
                          onChange={(_event, newValue) => onChange(newValue)}
                          filterSelectedOptions
                          isOptionEqualToValue={(option, value) =>
                            option === value
                          }
                          options={
                            !isEmpty(dataTypeOptions)
                              ? dataTypeOptions
                                  .sort()
                                  .map((option: any) => option)
                              : []
                          }
                          className="advanced-search-autocomplete"
                          renderInput={(params) => (
                            <TextField
                              {...params}
                              fullWidth
                              label="Select type"
                              InputProps={{
                                ...params.InputProps,
                                type: "search"
                              }}
                            />
                          )}
                        />
                      </Grid>{" "}
                    </Grid>
                  </>
                );
              }}
            />
          </fieldset>
          {enumModal && (
            <CustomModal
              open={enumModal}
              onClose={handleCloseEnumModal}
              title={"Create/ Update Enum"}
              button1Label="Cancel"
              button1Handler={handleCloseEnumModal}
              button2Label={"Update"}
              maxWidth="sm"
              button2Handler={handleSubmit(onSubmit)}
              disableButton2={isSubmitting}
            >
              <Stack gap={2} paddingTop="2rem" paddingBottom="2rem">
                <EnumCreateUpdate
                  control={enumControl}
                  handleSubmit={handleSubmit}
                  setValue={setValue}
                  reset={reset}
                  watch={watch}
                  handleCloseEnumModal={handleCloseEnumModal}
                />
              </Stack>
            </CustomModal>
          )}
        </>
      );
    }
  );
};

export default BusinessMetadataAttributeForm;
