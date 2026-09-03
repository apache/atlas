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
import React from "react";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import Button, { ButtonProps } from "@mui/material/Button";
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
import { TooltipProps } from "@mui/material/Tooltip";
import { SxProps, Theme } from "@mui/material/styles";

const LightTooltip = styled(({ className, ...props }: TooltipProps) => (
  <Tooltip
    sx={{ transition: "none" }}
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


interface OverflowTooltipProps extends Omit<TooltipProps, "children"> {
  children: React.ReactElement;
  wrapperSx?: SxProps<Theme>;
  wrapperClassName?: string;
}

const OverflowTooltip = ({ title, children, wrapperSx, wrapperClassName, ...props }: OverflowTooltipProps) => {
  const textElementRef = React.useRef<HTMLElement>(null);
  const [isOverflowed, setIsOverflowed] = React.useState(false);

  const checkOverflow = React.useCallback(() => {
    if (textElementRef.current) {
      const el = textElementRef.current;
      setIsOverflowed(
        el.scrollWidth > el.clientWidth || 
        el.scrollWidth > el.getBoundingClientRect().width
      );
    }
  }, []);

  React.useEffect(() => {
    checkOverflow();
    const element = textElementRef.current;
    if (element) {
      // One ResizeObserver per instance — acceptable for small lists (e.g. dashboard widgets).
      // If this component is used in large virtualized lists, consider lifting a shared
      // ResizeObserver to a context provider to reduce observer count.
      const resizeObserver = new ResizeObserver(() => checkOverflow());
      resizeObserver.observe(element);
      return () => resizeObserver.disconnect();
    }
  }, [title, checkOverflow]);

  const child = (
    <Box
      component="span"
      ref={textElementRef}
      className={wrapperClassName}
      sx={{
        display: "inline-flex",
        minWidth: 0,
        width: "100%",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
        ...wrapperSx
      }}
      onMouseEnter={checkOverflow}
    >
      {children}
    </Box>
  );

  return (
    <LightTooltip
      title={title}
      disableHoverListener={!isOverflowed}
      disableFocusListener={!isOverflowed}
      disableTouchListener={!isOverflowed}
      {...props}
    >
      {child}
    </LightTooltip>
  );
};

const ButtonWrapper = styled(Box)({
  display: "inline-flex"
});

const StyledButton = styled(Button)(({ variant }) => ({
  fontWeight: "600",
  letterSpacing: "0",
  fontSize: "0.875rem",
  cursor: "pointer",
  minWidth: "unset",
  ...(variant === "outlined" && { border: "1px solid #dddddd" })
}));

const CustomButton = ({
  children,
  sx,
  ...rest
}: ButtonProps) => {
  return (
    <ButtonWrapper component="span">
      <StyledButton sx={sx} {...rest}>
        {children}
      </StyledButton>
    </ButtonWrapper>
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
  AccordionDetails,
  OverflowTooltip
};
