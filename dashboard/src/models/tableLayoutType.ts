import { Cell, ColumnDef, Row } from "@tanstack/react-table";

export interface TableProps {
  fetchData?: any;
  data: any[];
  columns: ColumnDef<any>[];
  isFetching?: boolean;
  skeletonCount?: number;
  skeletonHeight?: number;
  headerComponent?: JSX.Element;
  pageCount?: number;
  defaultColumnVisibility?: any;
  page?: (page: number) => void;
  onClickRow?: (cell: Cell<any, unknown>, row: Row<any>) => void;
  emptyText?: string;
  children?: React.ReactNode | React.ReactElement;
  handleRow?: () => void;
  defaultColumnParams?: string;
  columnVisibility: boolean;
  currentpageIndex?: number;
  currentpageSize?: number;
  refreshTable?: () => void;
  defaultSortCol?: any;
  clientSideSorting?: boolean;
  columnSort: boolean;
  showPagination: boolean;
  showRowSelection: boolean;
  tableFilters?: boolean;
  expandRow?: boolean;
  auditTableDetails?: any;
  assignFilters?: { classifications: boolean; term: boolean } | null;
  queryBuilder?: boolean;
  allTableFilters?: boolean;
  columnVisibilityParams?: boolean;
  setUpdateTable?: any;
  isfilterQuery?: any;
  isClientSidePagination?: boolean;
}
