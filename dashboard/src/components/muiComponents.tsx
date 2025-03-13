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

import AutorenewIcon from "@mui/icons-material/Autorenew";
import { styled } from "@mui/material/styles";
import {
  CircularProgress,
  Switch,
  Divider,
  IconButton,
  ListItemIcon,
  Menu,
  MenuItem,
  Button,
  DialogTitle,
  DialogContent,
  DialogActions,
  Dialog,
  AccordionProps,
  AccordionSummaryProps,
  Tab
} from "@mui/material";
import MoreVertIcon from "@mui/icons-material/MoreVert";
import Tooltip, { tooltipClasses } from "@mui/material/Tooltip";
import FormatListBulletedIcon from "@mui/icons-material/FormatListBulleted";
import FileDownloadIcon from "@mui/icons-material/FileDownload";
import FileUploadIcon from "@mui/icons-material/FileUpload";
import AccountTreeIcon from "@mui/icons-material/AccountTree";
import Typography from "@mui/material/Typography";
import MenuIcon from "@mui/icons-material/Menu";
import Toolbar from "@mui/material/Toolbar";
import CloseIcon from "@mui/icons-material/Close";
import { Box } from "@mui/material";
import Zoom from "@mui/material/Zoom";
import MuiAccordion from "@mui/material/Accordion";
import MuiAccordionSummary from "@mui/material/AccordionSummary";
import ArrowForwardIosSharpIcon from "@mui/icons-material/ArrowForwardIosSharp";
import MuiAccordionDetails from "@mui/material/AccordionDetails";
import { LinkTabProps } from "@models/detailPageType";
import { samePageLinkNavigation } from "@utils/Muiutils";

const LightTooltip = styled(({ className, ...props }: any) => (
  <Tooltip
    sx={{ transition: "none" }}
    arrow
    {...props}
    classes={{ popper: className }}
    TransitionComponent={Zoom}
  />
))(({ theme }) => ({
  [`& .${tooltipClasses.tooltip}`]: {
    backgroundColor: theme.palette.common.white,
    color: "rgba(0, 0, 0, 0.87)",
    boxShadow: theme.shadows[1],
    fontSize: 11
  }
}));

interface ButtonProps {
  children?: any;
  variant?: string;
  color: string;
  onClick: any;
  sx?: any;
  size?: string;
  endIcon?: any;
  startIcon?: any;
  className?: string;
  disabled?: boolean;
}

const CustomButton = ({
  children,
  variant,
  color,
  sx: customStyles = {},
  onClick,
  size,
  endIcon,
  startIcon,
  disabled
}: ButtonProps | any) => {
  let defaultStyles = {
    fontWeight: "600",
    letterSpacing: "0",
    fontSize: "0.875rem",
    minWidth: "unset",
    ...(variant == "outlined" && { border: "1px solid #dddddd" })
  };

  let mergedStyle = { ...defaultStyles, ...customStyles };

  return (
    <Button
      variant={variant}
      color={color}
      sx={mergedStyle}
      onClick={onClick}
      size={size}
      endIcon={endIcon}
      startIcon={startIcon}
      disabled={disabled}
    >
      {children}
    </Button>
  );
};

const Accordion = styled((props: AccordionProps) => (
  <MuiAccordion disableGutters elevation={0} square {...props} />
))(({ theme }) => ({
  border: `1px solid ${theme.palette.divider}`,
  "&:not(:last-child)": {
    borderBottom: 0
  },
  "&::before": {
    display: "none"
  }
}));

const AccordionSummary = styled((props: AccordionSummaryProps) => (
  <MuiAccordionSummary
    expandIcon={<ArrowForwardIosSharpIcon className="accordion-icon" />}
    {...props}
  />
))(({ theme }) => ({
  backgroundColor:
    theme.palette.mode === "dark"
      ? "rgba(255, 255, 255, .05)"
      : "rgba(0, 0, 0, .03)",
  flexDirection: "row-reverse",
  "& .MuiAccordionSummary-expandIconWrapper.Mui-expanded": {
    transform: "rotate(90deg)"
  },
  "& .MuiAccordionSummary-content": {
    marginLeft: theme.spacing(1)
  }
}));
const AccordionDetails = styled(MuiAccordionDetails)(({ theme }) => ({
  padding: theme.spacing(2),
  borderTop: "1px solid rgba(0, 0, 0, .125)"
}));

export const LinkTab = (props: LinkTabProps) => {
  return (
    <Tab
      component="a"
      onClick={(event: React.MouseEvent<HTMLAnchorElement, MouseEvent>) => {
        if (samePageLinkNavigation(event)) {
          event.preventDefault();
        }
      }}
      aria-current={props.selected && "page"}
      {...props}
    />
  );
};

export {
  AutorenewIcon,
  CircularProgress,
  Switch,
  MoreVertIcon,
  Divider,
  Tooltip,
  LightTooltip,
  FormatListBulletedIcon,
  FileDownloadIcon,
  FileUploadIcon,
  AccountTreeIcon,
  IconButton,
  ListItemIcon,
  Menu,
  MenuItem,
  Typography,
  Box,
  MenuIcon,
  Toolbar,
  Button,
  DialogTitle,
  DialogContent,
  DialogActions,
  Dialog,
  CloseIcon,
  CustomButton,
  Accordion,
  AccordionSummary,
  AccordionDetails
};
