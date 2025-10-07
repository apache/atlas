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

import { CSSProperties, useRef } from "react";
import {
  Paper,
  Table as MuiTable,
  TableHead,
  TableCell,
  TableBody,
  TableRow,
  TableContainer,
  Checkbox,
  Stack,
  CheckboxProps,
  IconButton,
  Collapse,
  Typography,
  Divider
} from "@mui/material";
import {
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  PaginationState,
  SortingState,
  useReactTable
} from "@tanstack/react-table";
import { FC, useEffect, useMemo, useState } from "react";
import TableFilter from "./TableFilters";
import ArrowUpwardOutlinedIcon from "@mui/icons-material/ArrowUpwardOutlined";
import ArrowDownwardOutlinedIcon from "@mui/icons-material/ArrowDownwardOutlined";
import SwapVertOutlinedIcon from "@mui/icons-material/SwapVertOutlined";
import { useLocation, useSearchParams } from "react-router-dom";
import { isEmpty } from "../../utils/Utils";
import {
  DndContext,
  KeyboardSensor,
  MouseSensor,
  TouchSensor,
  closestCenter,
  type DragEndEvent,
  useSensor,
  useSensors
} from "@dnd-kit/core";
import { restrictToHorizontalAxis } from "@dnd-kit/modifiers";
import {
  arrayMove,
  SortableContext,
  horizontalListSortingStrategy
} from "@dnd-kit/sortable";
import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { TableProps } from "../../models/tableLayoutType";
import ChevronRightOutlinedIcon from "@mui/icons-material/ChevronRightOutlined";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import { CustomButton } from "../muiComponents";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
import TableRowsLoader from "./TableLoader";
import AddTag from "@views/Classification/AddTag";
import FilterQuery from "@components/FilterQuery";
import TablePagination from "./TablePagination";

interface IndeterminateCheckboxProps extends Omit<CheckboxProps, "ref"> {
  indeterminate?: boolean;
  className?: string;
}

const IndeterminateCheckbox: FC<IndeterminateCheckboxProps> = ({
  indeterminate,
  className = "",
  ...rest
}) => {
  const ref = useRef<HTMLInputElement>(null!);

  useEffect(() => {
    if (typeof indeterminate === "boolean") {
      if (ref.current != null) {
        ref.current.indeterminate = !rest.checked && indeterminate;
      }
    }
  }, [ref, indeterminate]);

  return (
    <Checkbox
      sx={{
        color: "rgb(140, 140, 140)",
        "&.Mui-checked": {
          color: "rgb(25, 118, 210)"
        }
      }}
      {...rest}
      color="primary"
      inputRef={ref}
      className={className + " cursor-pointer"}
      size="small"
    />
  );
};

const Row = ({
  row,
  handleRow,
  showRowSelection,
  expandRow,
  onClickRow,
  columnOrder,
  auditTableDetails
}: any) => {
  const [openModule, setOpenModule] = useState(false);
  const { Component = {}, componentProps = {} } = auditTableDetails || {};

  return (
    <>
      <TableRow hover key={row.id} onClick={handleRow}>
        {showRowSelection && (
          <TableCell width="48">
            <IndeterminateCheckbox
              {...{
                checked: row.getIsSelected(),
                disabled: !row.getCanSelect(),
                indeterminate: row.getIsSomeSelected(),
                onChange: row.getToggleSelectedHandler()
              }}
            />
          </TableCell>
        )}
        {expandRow && (
          <TableCell width="48">
            <IconButton
              aria-label="expand row"
              size="small"
              onClick={() => setOpenModule(!openModule)}
            >
              {openModule ? (
                <KeyboardArrowUpIcon color="success" />
              ) : (
                <ChevronRightOutlinedIcon color="success" />
              )}
            </IconButton>
          </TableCell>
        )}

        {row.getVisibleCells().map((cell: { id: null | undefined }) => (
          <SortableContext
            key={cell.id}
            items={columnOrder}
            strategy={horizontalListSortingStrategy}
          >
            <DragAlongCell
              key={cell.id}
              cell={cell}
              onClickRow={onClickRow}
              row={row}
            />
          </SortableContext>
        ))}
      </TableRow>
      {expandRow && (
        <TableRow hover onClick={handleRow}>
          <TableCell style={{ padding: 0 }} colSpan={10}>
            <Collapse in={openModule} timeout="auto" unmountOnExit>
              <Stack
                className="properties-container"
                key={row.index}
                direction="column"
                margin={0}
                padding={2}
              >
                <Component componentProps={componentProps} row={row} />
              </Stack>
            </Collapse>
          </TableCell>
        </TableRow>
      )}
    </>
  );
};

const DraggableTableHeader = ({ header, isEmptyRows }: { header: any; isEmptyRows?: boolean }) => {
  const { attributes, isDragging, listeners, setNodeRef, transform } =
    useSortable({
      id: header.column.id
    });

  const style: CSSProperties = {
    opacity: isDragging ? 0.8 : 1,
    position: "relative",
    transform: CSS.Translate.toString(transform),
    transition: "width transform 0.2s ease-in-out",
    whiteSpace: "nowrap",
    width: isEmptyRows
      ? undefined
      : header.column.columnDef?.width != undefined
      ? header.column.columnDef?.width
      : header.column.getSize(),
    zIndex: isDragging ? 1 : 0
  };

  return (
    <TableCell
      colSpan={header.colSpan}
      key={header.id}
      className="text-white text-sm font-cambon table-header-cell flex-1"
      ref={setNodeRef}
      style={style}
    >
      {header.isPlaceholder ? null : (
        <div
          className={`${
            header.column.getCanSort() ? "cursor-pointer select-none" : ""
          } table-header-wrap`}
          onClick={header.column.getToggleSortingHandler()}
          title={
            header.column.getCanSort()
              ? header.column.getNextSortingOrder() === "asc"
                ? "Sort ascending"
                : header.column.getNextSortingOrder() === "desc"
                ? "Sort descending"
                : "Clear sort"
              : undefined
          }
        >
          <span
            className="table-header-text"
            {...attributes}
            {...listeners}
          >
            {" "}
            {flexRender(header.column.columnDef.header, header.getContext())}
          </span>
          {{
            asc: (
              <ArrowUpwardOutlinedIcon
                style={{
                  fontSize: "1rem",
                  display: "block"
                }}
              />
            ),
            desc: (
              <ArrowDownwardOutlinedIcon
                style={{
                  fontSize: "1rem",
                  display: "block"
                }}
              />
            ),
            false: header.column.getCanSort() &&
              header.column.getIsSorted() == false && (
                <SwapVertOutlinedIcon
                  style={{
                    fontSize: "1rem",
                    display: "block"
                  }}
                />
              )
          }[header.column.getIsSorted() as string] ?? null}{" "}
        </div>
      )}
    </TableCell>
  );
};

const DragAlongCell = ({
  cell,
  onClickRow,
  row
}: {
  cell: any;
  onClickRow: any;
  row: any;
}) => {
  const { isDragging, setNodeRef, transform } = useSortable({
    id: cell.column.id
  });

  const style: CSSProperties = {
    opacity: isDragging ? 0.8 : 1,
    position: "relative",
    transform: CSS.Translate.toString(transform),
    transition: "width transform 0.2s ease-in-out",
    width:
      cell.column.columnDef.width != undefined
        ? cell.column.columnDef.width
        : cell.column.getSize(),
    zIndex: isDragging ? 1 : 0,
    padding: "8px",
    fontSize: "14px !important"
  };

  return (
    <TableCell
      onClick={() => onClickRow?.(cell, row)}
      key={cell.id}
      sx={{
        padding: "8px",
        fontSize: "14px !important"
      }}
      className="text-[#2E353A] text-base font-graphik table-body-cell"
      style={style}
      ref={setNodeRef}
    >
      {flexRender(cell.column.columnDef.cell, cell.getContext())}
    </TableCell>
  );
};

const TableLayout: FC<TableProps> = ({
  fetchData,
  data,
  columns,
  isFetching,
  defaultColumnVisibility,
  pageCount,
  totalCount,
  onClickRow,
  emptyText,
  defaultColumnParams,
  handleRow,
  columnVisibility: isColumnVisible,
  refreshTable,
  defaultSortCol,
  clientSideSorting,
  columnSort,
  showRowSelection,
  tableFilters,
  expandRow,
  auditTableDetails,
  assignFilters,
  queryBuilder,
  allTableFilters,
  columnVisibilityParams,
  showPagination,
  setUpdateTable,
  isfilterQuery,
  isClientSidePagination,
  isEmptyData,
  setIsEmptyData,
  showGoToPage
}) => {
  let defaultHideColumns = { ...defaultColumnVisibility };
  const location = useLocation();
  const memoizedData = useMemo(() => data, [data]);
  const memoizedColumns = useMemo(() => columns, [columns]);
  const [searchParams] = useSearchParams();
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: 25
  });

  const [goToPageVal, setGoToPageVal] = useState<any>("");

  const [rowSelection, setRowSelection] = useState({});
  const existingColumnIds = useMemo(
    () =>
      (!isEmpty(memoizedColumns)
        ? memoizedColumns.map((c: any) => c.id || c.accessorKey)
        : []) as string[],
    [memoizedColumns]
  );

  const sanitizeSorting = (sortingState: SortingState) => {
    if (isEmpty(sortingState)) return [] as SortingState;
    return sortingState.filter(
      (s: any) => s && existingColumnIds.includes(s.id)
    );
  };

  const [sorting, setSorting] = useState<SortingState>(() => []);

  const [columnOrder, setColumnOrder] = useState<any>(() =>
    !isEmpty(memoizedColumns)
      ? memoizedColumns
          .map((c: any) => c.id || c.accessorKey)
          .filter(Boolean)
      : []
  );

  useEffect(() => {
    setColumnOrder(
      !isEmpty(memoizedColumns)
        ? memoizedColumns
            .map((c: any) => c.id || c.accessorKey)
            .filter(Boolean)
        : []
    )
  }, [memoizedColumns])
  const [columnVisibility, setColumnVisibility] = useState(defaultHideColumns);
  const [tagModal, setTagModal] = useState<boolean>(false);
  let currentParams = searchParams;
  let typeParam = searchParams.get("type");
  const params: any = {};
  currentParams.forEach((value, key) => {
    params[key] = value;
  });
  // Total number of visible table columns including selection/expand controls
  const totalVisibleColumns =
    (showRowSelection ? 1 : 0) + (expandRow ? 1 : 0) + columnOrder.length;
  const {
    getHeaderGroups,
    getRowModel,
    setPageIndex,
    getPageCount,
    nextPage,
    previousPage,
    setPageSize,
    resetSorting,
    resetRowSelection,
    getIsAllRowsSelected,
    getIsSomeRowsSelected,
    getToggleAllRowsSelectedHandler,
    getAllLeafColumns,
    getSelectedRowModel
  } = useReactTable({
    data: memoizedData,
    columns: memoizedColumns,
    enableRowSelection: true,
    manualSorting: !clientSideSorting && true,
    enableSortingRemoval: false,
    enableSorting: columnSort,
    onColumnVisibilityChange: setColumnVisibility,
    onRowSelectionChange: setRowSelection,
    getCoreRowModel: getCoreRowModel(),
    onSortingChange: setSorting,
    onColumnOrderChange: setColumnOrder,
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: isClientSidePagination
      ? getPaginationRowModel()
      : undefined,
    onPaginationChange: setPagination,
    state: {
      columnVisibility: columnVisibilityParams
        ? defaultHideColumns
        : columnVisibility,
      sorting: sanitizeSorting(sorting),
      pagination,
      rowSelection,
      columnOrder
    },
    debugTable: true,
    manualPagination: !isClientSidePagination && true,
    pageCount,
    autoResetPageIndex: false
  });

  useEffect(() => {
    setSorting((prev) => sanitizeSorting(prev));
    setColumnOrder(
      !isEmpty(memoizedColumns)
        ? memoizedColumns
            .map((c: any) => c.id || c.accessorKey)
            .filter(Boolean)
        : []
    );
  }, [existingColumnIds]);

  useEffect(() => {
    if (typeof fetchData === "function") {
      fetchData({
        pagination,
        sorting
      });
    }
  }, [
    fetchData,
    pagination.pageIndex,
    pagination.pageSize,
    clientSideSorting ? null : sorting
  ]);

  function handleDragEnd(event: DragEndEvent) {
    const { active, over } = event;
    if (active && over && active.id !== over.id) {
      setColumnOrder((columnOrder: any) => {
        const oldIndex = columnOrder.indexOf(active.id as string);
        const newIndex = columnOrder.indexOf(over.id as string);
        return arrayMove(columnOrder, oldIndex, newIndex);
      });
    }
  }

  const sensors = useSensors(
    useSensor(MouseSensor, {}),
    useSensor(TouchSensor, {}),
    useSensor(KeyboardSensor, {})
  );

  const [, setSearchParams] = useSearchParams();

  useEffect(() => {
    resetSorting(true);
    resetRowSelection(true);
    setRowSelection({});
    const candidate = !isEmpty(defaultSortCol) ? defaultSortCol : []
    setSorting(sanitizeSorting(candidate));
  }, [typeParam, defaultSortCol, setSearchParams]);

  const handleCloseTagModal = () => {
    setTagModal(false);
  };
  const selectedRow = !isEmpty(getSelectedRowModel()?.rows)
    ? getSelectedRowModel()?.rows?.map((obj) => {
        return obj.original;
      })
    : [];

  return (
    <>
      <Stack gap="0.5rem">
        {tableFilters && (
          <Paper
            className="table-paper checkbox-table"
            variant="outlined"
            sx={{
              boxShadow: "none !important",
              padding: "12px 16px",
              minHeight: "55px",
              backgroundColor: "rgba(255,255,255,0.6)",
              borderRadius: "4px"
            }}
          >
            <Stack gap="0.75rem">
              {tableFilters && (
                <TableFilter
                  getAllColumns={getAllLeafColumns}
                  defaultColumnParams={defaultColumnParams}
                  columnVisibility={isColumnVisible}
                  columnVisibilityParams={columnVisibilityParams}
                  refreshTable={refreshTable}
                  rowSelection={rowSelection}
                  setRowSelection={setRowSelection}
                  queryBuilder={queryBuilder}
                  allTableFilters={allTableFilters}
                  setUpdateTable={setUpdateTable}
                  getSelectedRowModel={getSelectedRowModel}
                  memoizedData={memoizedData}
                />
              )}
              {isfilterQuery && <Divider />}
              {isfilterQuery &&
                (location.pathname == "/search/searchResult" ||
                  location.pathname ==
                    "/relationship/relationshipSearchresult") && (
                  <Stack
                    flexWrap="wrap"
                    direction="row"
                    gap="0.25rem"
                    alignItems="center"
                  >
                    <FilterQuery value={params} />
                  </Stack>
                )}

              {assignFilters && (
                <Stack
                  direction="row"
                  spacing={1}
                  position="absolute"
                  top={0}
                  left={0}
                >
                  {!isEmpty(rowSelection) && (
                    <>
                      {assignFilters.term && (
                        <CustomButton
                          variant="outlined"
                          color="success"
                          classes="table-filter-btn"
                          size="small"
                          onClick={() => {
                            setRowSelection({});
                          }}
                          startIcon={<AddOutlinedIcon />}
                        >
                          Term
                        </CustomButton>
                      )}
                      {assignFilters.classifications && (
                        <CustomButton
                          variant="outlined"
                          color="success"
                          classes="table-filter-btn"
                          size="small"
                          onClick={() => {
                            setTagModal(true);
                          }}
                          startIcon={<AddOutlinedIcon />}
                        >
                          Classification
                        </CustomButton>
                      )}
                    </>
                  )}
                </Stack>
              )}
            </Stack>
          </Paper>
        )}
        <Paper
          className="table-paper checkbox-table"
          variant="outlined"
          sx={{ boxShadow: "none !important" }}
        >
          <TableContainer>
            <DndContext
              collisionDetection={closestCenter}
              modifiers={[restrictToHorizontalAxis]}
              onDragEnd={handleDragEnd}
              sensors={sensors}
            >
              <MuiTable
                size="small"
                className={`table ${expandRow ? "expand-row-table" : ""} ${
                  memoizedData && memoizedData.length > 0 && expandRow
                    ? "has-expanded-rows"
                    : ""
                }`}
                sx={{
                  tableLayout: memoizedData.length > 0 ? "fixed" : "auto",
                  width: "100%"
                }}
              >
                {/* remove dynamic colgroup for empty state */}
                {!isFetching && (
                  <TableHead>
                    {getHeaderGroups().map((headerGroup) => (
                      <TableRow
                        hover
                        key={headerGroup.id}
                        className="table-header-row"
                      >
                        {showRowSelection && (
                          <TableCell width="48">
                            <IndeterminateCheckbox
                              {...{
                                checked: getIsAllRowsSelected(),
                                indeterminate: getIsSomeRowsSelected(),
                                onChange: getToggleAllRowsSelectedHandler()
                              }}
                            />
                          </TableCell>
                        )}
                        {expandRow && <TableCell width="48" />}
                        <SortableContext
                          items={columnOrder}
                          strategy={horizontalListSortingStrategy}
                        >
                          
                          {headerGroup.headers.map((header) =>
                            header.isPlaceholder ? null : (
                              <DraggableTableHeader
                                key={header.id}
                                header={header}
                                isEmptyRows={!isFetching && memoizedData.length === 0}
                              />
                            )
                          )}
                        </SortableContext>
                      </TableRow>
                    ))}
                  </TableHead>
                )}
                <TableBody>
                  {isFetching ? (
                    <TableRowsLoader rowsNum={10} />
                  ) : memoizedData.length === 0 && isFetching == false ? (
                    <TableRow>
                      <TableCell colSpan={Math.max(1, totalVisibleColumns)}>
                        <Stack textAlign="center">
                          <Typography fontWeight="600" color="text.secondary">
                            {emptyText}
                          </Typography>
                        </Stack>
                      </TableCell>
                    </TableRow>
                  ) : (
                    getRowModel()?.rows.map((row) => (
                      <Row
                        key={row.id}
                        row={row}
                        handleRow={handleRow}
                        showRowSelection={showRowSelection}
                        expandRow={expandRow}
                        onClickRow={onClickRow}
                        columnOrder={columnOrder}
                        auditTableDetails={auditTableDetails}
                      />
                    ))
                  )}
                </TableBody>
              </MuiTable>
            </DndContext>
          </TableContainer>

          {showPagination && !isFetching && (
            <TablePagination
              isServerSide={!isClientSidePagination}
              getPageCount={getPageCount}
              setPageIndex={setPageIndex}
              setPageSize={setPageSize}
              nextPage={nextPage}
              previousPage={previousPage}
              getRowModel={getRowModel}
              pagination={pagination}
              setRowSelection={setRowSelection}
              memoizedData={memoizedData}
              isFirstPage={pagination.pageIndex === 0}
              setPagination={setPagination}
              goToPageVal={goToPageVal}
              setGoToPageVal={setGoToPageVal}
              isEmptyData={isEmptyData}
              setIsEmptyData={setIsEmptyData}
              showGoToPage={showGoToPage}
              totalCount={totalCount}
            />
          )}
        </Paper>
      </Stack>
      {tagModal && (
        <div style={{ position: "absolute" }}>
          <AddTag
            open={tagModal}
            isAdd={true}
            entityData={selectedRow}
            onClose={handleCloseTagModal}
            setUpdateTable={setUpdateTable}
            setRowSelection={setRowSelection}
          />
        </div>
      )}
    </>
  );
};

export { TableLayout, IndeterminateCheckbox };
