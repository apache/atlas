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

import { styled } from "@mui/material/styles";
import { tooltipClasses } from "@mui/material/Tooltip";
import CircularProgress from "@mui/material/CircularProgress";
import Switch from "@mui/material/Switch";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import Button from "@mui/material/Button";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import DialogActions from "@mui/material/DialogActions";
import Dialog from "@mui/material/Dialog";
import Tab from "@mui/material/Tab";
import Tooltip from "@mui/material/Tooltip";
import Zoom from "@mui/material/Zoom";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import Toolbar from "@mui/material/Toolbar";
import AutorenewIcon from "@mui/icons-material/Autorenew";
import MoreVertIcon from "@mui/icons-material/MoreVert";
import FormatListBulletedIcon from "@mui/icons-material/FormatListBulleted";
import FileDownloadIcon from "@mui/icons-material/FileDownload";
import FileUploadIcon from "@mui/icons-material/FileUpload";
import AccountTreeIcon from "@mui/icons-material/AccountTree";
import MenuIcon from "@mui/icons-material/Menu";
import CloseIcon from "@mui/icons-material/Close";
import ArrowForwardIosSharpIcon from "@mui/icons-material/ArrowForwardIosSharp";
import { LinkTabProps } from "@models/detailPageType";
import { samePageLinkNavigation } from "@utils/Muiutils";
import MuiAccordion, { AccordionProps } from "@mui/material/Accordion";
import MuiAccordionSummary, {
  AccordionSummaryProps
} from "@mui/material/AccordionSummary";
import MuiAccordionDetails from "@mui/material/AccordionDetails";

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
    fontWeight: "600 !important",
    letterSpacing: "0 !important",
    fontSize: "0.875rem !important",
    cursor: "pointer !important",
    minWidth: "unset !important",
    ...(variant == "outlined" && { border: "1px solid #dddddd !important" })
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
