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

import { useEffect, useRef, useState } from "react";
import { useDropzone } from "react-dropzone";
import { Typography, LinearProgress, Stack } from "@mui/material";
import { toast } from "react-toastify";
import { CustomButton } from "@components/muiComponents";

const thumb = {
  position: "relative",
  display: "inline-flex",
  borderRadius: "20px",
  border: "1px solid #eaeaea",
  width: 144,
  height: 144,
  margin: "0 !important",
  padding: 2,
  boxSizing: "border-box",
  backgroundSize: "cover",
  backgroundPosition: "center",
  background: "linear-gradient(to bottom, #eee, #ddd)"
};

const thumbInner = {
  display: "flex",
  minWidth: "100%",
  overflow: "hidden",
  flexDirection: "column",
  alignItems: "center",
  justifyContent: "center",
  gap: "0.25rem"
};

const progressCss = {
  width: "100%",
  height: "16px",
  borderRadius: "8px"
};

const removeButton = {
  position: "relative",
  bottom: "4px",
  textTransform: "none",
  "&:hover": {
    textDecoration: "underline"
  }
};

interface FileWithPreview extends File {
  preview: string;
}

const ImportLayout = ({
  setFileData,
  progressVal,
  setProgress,
  selectedFile,
  errorDetails
}: {
  setFileData: any;
  progressVal: number;
  setProgress: any;
  selectedFile: any;
  errorDetails: boolean;
}) => {
  const [files, setFiles] = useState<FileWithPreview[]>(
    !errorDetails ? selectedFile : []
  );
  const [hasFiles, setHasFiles] = useState(false);
  const toastId: any = useRef(null);

  const { getRootProps, getInputProps } = useDropzone({
    accept: { "text/csv": [".csv", ".xls", ".xlsx"] },
    multiple: false,
    maxFiles: 1,
    onDrop: (acceptedFiles) => {
      if (files.length != 0) {
        toast.dismiss(toastId.current);
        toastId.current = toast.error("You can not upload any more files..");
      }

      setFiles(
        acceptedFiles.map((file: File) =>
          Object.assign(file, {
            preview: URL.createObjectURL(file)
          })
        )
      );
      setProgress(0);
      if (acceptedFiles.length == 0) {
        toast.dismiss(toastId.current);
        toastId.current = toast.error("You can't upload files of this type.");
      }
    }
  });

  useEffect(() => {
    setHasFiles(files.length > 0);
    setFileData(files[0]);
  }, [files]);

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizeInKB = (bytes / k).toFixed(1);
    const sizeInMB = (bytes / (k * k)).toFixed(1);
    if (bytes < 100) {
      return `${bytes} b`;
    } else if (bytes > 100 && +sizeInKB < 100) {
      return `${sizeInKB} KB`;
    } else if (+sizeInKB > 100) {
      return `${sizeInMB} MB`;
    }
  };

  const thumbs = files.map((file: FileWithPreview) => (
    <>
      <Stack key={file.name} sx={thumb}>
        <Stack sx={thumbInner}>
          <Typography>{formatFileSize(file.size)}</Typography>

          <LinearProgress
            value={progressVal}
            variant="determinate"
            color="inherit"
            sx={progressCss}
          />
          <Typography
            sx={{
              overflow: "hidden",
              whiteSpace: "nowrap",
              textOverflow: "ellipsis",
              lineHeight: "2",
              width: "100%"
            }}
          >
            {file.name}
          </Typography>
        </Stack>
      </Stack>
      <CustomButton
        color="success"
        aria-label="remove"
        onClick={(e: any) => {
          e.stopPropagation();
          handleRemoveFile(file);
        }}
        sx={removeButton}
        size="small"
      >
        {progressVal > 0 && progressVal < 100 ? "Cancel Upload" : "Remove file"}
      </CustomButton>
    </>
  ));

  const handleRemoveFile = (file: FileWithPreview) => {
    const updatedFiles = files.filter((f) => f !== file);
    setFiles(updatedFiles);
  };

  return (
    <section>
      <Stack
        {...getRootProps({ className: "dropzone" })}
        sx={{
          cursor: "pointer",
          padding: "20px",
          border: "2px dashed #4a90e2",
          borderRadius: "10px",
          textAlign: "center",
          "&:focus": {
            outline: "none",
            borderColor: "#2196f3"
          },
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
          alignItems: "center",
          height: "224px",
          position: "relative"
        }}
        data-cy="importGlossary"
      >
        <input {...getInputProps()} />
        {hasFiles ? (
          thumbs
        ) : (
          <Typography color="grey" variant="h6" align="center">
            Drop files here or click to upload(.csv, .xls, .xlsx).
          </Typography>
        )}
      </Stack>
    </section>
  );
};

export default ImportLayout;
