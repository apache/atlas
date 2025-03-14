import React from "react";
import { Skeleton } from "@mui/material";

type SkeletonLoaderProps = {
  count: number;
  variant?: "text" | "rectangular" | "circular";
  animation?: "pulse" | "wave" | false;
  width?: string | number;
  height?: string | number;
  sx?: object;
  className?: string;
};

const SkeletonLoader: React.FC<SkeletonLoaderProps> = ({
  count,
  animation,
  variant,
  width,
  height,
  sx,
  className
}) => {
  const intArr = Array.from({ length: count }, (_, index) => index);
  return intArr.map((num: number) => {
    return (
      <Skeleton
        sx={sx}
        className={className}
        key={num}
        animation={animation}
        variant={variant}
        width={width}
        height={height}
        component="div"
      />
    );
  });
};

export default SkeletonLoader;
