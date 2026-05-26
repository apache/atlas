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

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { isEmpty } from '@utils/Utils'
import { CircularProgress, LightTooltip } from '@components/muiComponents'

type RelationshipCardProps = {
	attributeName: string
	data: any[]
	referredEntities: any
	showEmptyValues: boolean
	showTypeNameInDisplay?: boolean
	onLoadMore?: (attributeName: string) => void
	totalCount?: number
	isLoading?: boolean
	isResetting?: boolean
	isSorted?: boolean
	showDeleted?: boolean
	pageLimit?: number
	onToggleSort?: (attributeName: string) => void
	onToggleShowDeleted?: (attributeName: string) => void
	onPageLimitChange?: (attributeName: string, value: string) => void
	onPageLimitSubmit?: (attributeName: string, rawValue: string) => void
}

function RelationshipCard({
	attributeName,
	data,
	referredEntities,
	showEmptyValues: _showEmptyValues,
	showTypeNameInDisplay = false,
	onLoadMore,
	totalCount,
	isLoading,
	isResetting,
	isSorted,
	showDeleted,
	pageLimit,
	onToggleSort,
	onToggleShowDeleted,
	onPageLimitChange,
	onPageLimitSubmit
}: RelationshipCardProps) {
	const scrollRef = useRef<HTMLDivElement | null>(null)
	const resolvedTotal = useMemo(() => {
		if (typeof totalCount === 'number') return totalCount
		return data.length
	}, [totalCount, data.length])

	const minPageLimit = resolvedTotal <= 1 ? 1 : 2

	const canLoadMore = resolvedTotal > data.length
	const lastRequestSizeRef = useRef<number | null>(null)
	const prevScrollRef = useRef<{ height: number; top: number } | null>(null)
	const lastScrollTopRef = useRef<number>(0)
	const loadRequestedRef = useRef<boolean>(false)
	const scrollDebounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(
		null
	)
	const [searchQuery, setSearchQuery] = useState<string>('')

	const getDisplayText = useCallback(
		(item: any) => {
			const ref = item?.guid ? referredEntities?.[item.guid] : null
			const displayText =
				ref?.displayText ||
				ref?.attributes?.name ||
				item?.displayText ||
				item?.attributes?.name ||
				item?.qualifiedName ||
				item?.guid ||
				'N/A'
			const typeName = ref?.typeName || item?.typeName || ''
			if (showTypeNameInDisplay && typeName) {
				return `${displayText} (${typeName})`
			}
			return displayText
		},
		[referredEntities, showTypeNameInDisplay]
	)

	const handleSearchChange = useCallback(
		(event: React.ChangeEvent<HTMLInputElement>) => {
			setSearchQuery(event.currentTarget.value)
		},
		[]
	)

	const handleSearchKeyDown = useCallback(
		(event: React.KeyboardEvent<HTMLInputElement>) => {
			if (event.key === 'Escape') {
				setSearchQuery('')
			}
		},
		[]
	)

	const handleSearchKeyUp = useCallback(
		(event: React.KeyboardEvent<HTMLInputElement>) => {
			setSearchQuery(event.currentTarget.value)
		},
		[]
	)

	const handleKeyDown = (
		event: React.KeyboardEvent<HTMLElement>,
		href?: string
	) => {
		if (!href) return
		if (event.key === 'Enter' || event.key === ' ') {
			event.preventDefault()
			window.location.href = href
		}
	}

	const handleSortToggle = () => {
		onToggleSort?.(attributeName)
	}

	const handleShowDeletedToggle = (
		event: React.ChangeEvent<HTMLInputElement>
	) => {
		event.stopPropagation()
		onToggleShowDeleted?.(attributeName)
	}

	const handlePageLimitInputChange = (
		event: React.ChangeEvent<HTMLInputElement>
	) => {
		onPageLimitChange?.(attributeName, event.currentTarget.value)
	}

	const handlePageLimitKeyDown = (
		event: React.KeyboardEvent<HTMLInputElement>
	) => {
		if (event.key === 'Enter') {
			onPageLimitSubmit?.(attributeName, event.currentTarget.value)
		}
	}

	const handleLoadMore = useCallback(() => {
		if (loadRequestedRef.current || !canLoadMore) {
			return
		}
		loadRequestedRef.current = true
		const container = scrollRef.current
		if (container) {
			prevScrollRef.current = {
				height: container.scrollHeight,
				top: container.scrollTop
			}
		}
		onLoadMore?.(attributeName)
	}, [canLoadMore, onLoadMore])

	const handleScroll = useCallback(() => {
		const container = scrollRef.current
		if (!container || loadRequestedRef.current || !canLoadMore) {
			return
		}
		const distanceFromBottom =
			container.scrollHeight - container.scrollTop - container.clientHeight
		const isNearBottom = distanceFromBottom <= 4
		const isScrollingDown = container.scrollTop > lastScrollTopRef.current
		lastScrollTopRef.current = container.scrollTop
		if (!isNearBottom || !isScrollingDown) {
			if (scrollDebounceTimerRef.current) {
				clearTimeout(scrollDebounceTimerRef.current)
				scrollDebounceTimerRef.current = null
			}
			return
		}
		if (lastRequestSizeRef.current === data.length) {
			return
		}
		if (scrollDebounceTimerRef.current) {
			clearTimeout(scrollDebounceTimerRef.current)
		}
		scrollDebounceTimerRef.current = setTimeout(() => {
			scrollDebounceTimerRef.current = null
			if (loadRequestedRef.current || lastRequestSizeRef.current === data.length) {
				return
			}
			lastRequestSizeRef.current = data.length
			handleLoadMore()
		}, 400)
	}, [canLoadMore, data.length, handleLoadMore])

	useEffect(() => {
		if (!isLoading) {
			loadRequestedRef.current = false
		}
	}, [isLoading])

	useEffect(() => {
		return () => {
			if (scrollDebounceTimerRef.current) {
				clearTimeout(scrollDebounceTimerRef.current)
				scrollDebounceTimerRef.current = null
			}
		}
	}, [])

	useEffect(() => {
		lastRequestSizeRef.current = null
		const container = scrollRef.current
		const prevScroll = prevScrollRef.current
		if (container && prevScroll) {
			prevScrollRef.current = null
			const applyScroll = () => {
				const maxScroll = container.scrollHeight - container.clientHeight
				if (maxScroll > 0) {
					const scrollUpBy = Math.floor(container.clientHeight * 0.3)
					container.scrollTop = Math.max(0, maxScroll - scrollUpBy)
				} else {
					container.scrollTop = 0
				}
			}
			requestAnimationFrame(() => {
				requestAnimationFrame(applyScroll)
			})
		}
	}, [data.length])

	const showSearch = data.length > 1
	const isEmptyCard = resolvedTotal === 0 && isEmpty(data)
	const isZeroOrOneRecord = data.length <= 1
	const bodyStyle = useMemo(() => {
		const maxVisibleRows = 9
		const rowHeight = 22
		const bodyPadding = 16
		const maxHeight = maxVisibleRows * rowHeight + bodyPadding

		if (isZeroOrOneRecord && !canLoadMore) {
			return { minHeight: 72 }
		}

		if (canLoadMore) {
			const visibleRows = Math.min(Math.max(data.length, 1), maxVisibleRows)
			let baseHeight = visibleRows * rowHeight + bodyPadding
			baseHeight = Math.max(baseHeight, 80)
			const h = Math.min(baseHeight, maxHeight)
			return {
				height: h,
				maxHeight: h,
				minHeight: 0,
				overflowY: 'auto' as const,
			}
		}

		const visibleRows = Math.min(data.length, maxVisibleRows)
		const baseHeight = visibleRows * rowHeight + bodyPadding
		return { height: Math.min(baseHeight, maxHeight), minHeight: 0 }
	}, [data.length, isZeroOrOneRecord, canLoadMore])

	const filteredData = useMemo(() => {
		const query = searchQuery.trim().toLowerCase()
		if (!query) return data
		return data.filter((item) => {
			const displayText = getDisplayText(item)
			const qualifiedName = item?.qualifiedName || ''
			const name = item?.attributes?.name || ''
			const searchTarget = `${displayText} ${qualifiedName} ${name}`.toLowerCase()
			return searchTarget.includes(query)
		})
	}, [data, getDisplayText, searchQuery])

	return (
		<div className='relationship-card'>
			<div className='relationship-card__header'>
				<div className='relationship-card__title'>
					{attributeName}
					<span className='relationship-card__count'>
						({resolvedTotal})
					</span>
				</div>
				<div className='relationship-card__actions'>
					<button
						type='button'
						className={`relationship-card__action-button${
							isSorted ? ' is-active' : ''
						}`}
						onClick={handleSortToggle}
						aria-label='Toggle sort by name'
					>
						<i className={`fa ${isSorted ? 'fa-sort-alpha-asc' : 'fa-sort'}`} />
					</button>
					<label className='relationship-card__toggle'>
						<input
							type='checkbox'
							checked={!!showDeleted}
							onChange={handleShowDeletedToggle}
							aria-label='Show deleted relationships'
						/>
						<span>Show deleted</span>
					</label>
				</div>
			</div>
			<div className='relationship-card__content'>
				{showSearch && (
					<div className='relationship-card__search'>
						<input
							className='relationship-card__search-input'
							type='text'
							placeholder='Search...'
							aria-label='Search relationship records'
							value={searchQuery}
							onChange={handleSearchChange}
							onKeyDown={handleSearchKeyDown}
							onKeyUp={handleSearchKeyUp}
						/>
					</div>
				)}
				<div
					ref={scrollRef}
					onScroll={handleScroll}
					className={`relationship-card__body${
						isEmptyCard ? ' relationship-card__body--empty' : ''
					}`}
					style={bodyStyle}
				>
					{isResetting ? (
						<div className='relationship-card__inline-loader'>
							<CircularProgress size={16} />
						</div>
					) : isEmpty(data) ? (
						<div className='relationship-card__empty'>
							No records to display
						</div>
					) : searchQuery.trim() && isEmpty(filteredData) ? (
						<div className='relationship-card__no-match'>
							No record found for "{searchQuery}"
						</div>
					) : (
						<ul className='relationship-card__list'>
							{filteredData.map((item, index) => {
								const itemGuid = item?.guid
								const href = itemGuid ? `/detailPage/${itemGuid}` : ''
								const displayText = getDisplayText(item)
								const ref = itemGuid ? referredEntities?.[itemGuid] : null
								const status =
									ref?.status || item?.status || item?.attributes?.status || ''
								const isDeleted = status === 'DELETED'
								const linkClass = isDeleted
									? 'relationship-card__link relationship-card__link--deleted'
									: 'relationship-card__link'
								const textClass = isDeleted
									? 'relationship-card__text relationship-card__text--deleted'
									: 'relationship-card__text'
								return (
									<li
										key={`${attributeName}-${index}`}
										className='relationship-card__item'
									>
										{href ? (
											<LightTooltip title={displayText}>
												<Link
													to={href}
													tabIndex={0}
													aria-label={`Open ${displayText}`}
													onKeyDown={(event) =>
														handleKeyDown(event, href)
													}
													className={linkClass}
												>
													{displayText}
												</Link>
											</LightTooltip>
										) : (
											<span className={textClass}>
												{displayText}
											</span>
										)}
									</li>
								)
							})}
						</ul>
					)}
					{canLoadMore && resolvedTotal > 0 && (
						<div className='relationship-card__load-more'>
							Scroll to load more data
						</div>
					)}
					{isLoading && (
						<div className='relationship-card__loading'>
							<CircularProgress size={16} />
						</div>
					)}
				</div>
				<div className='relationship-card__footer'>
					<div className='relationship-card__footer-row'>
						<span>
							Showing {data.length} of {resolvedTotal}
						</span>
						<div className='relationship-card__page-limit'>
							<label htmlFor={`page-limit-${attributeName}`}>
								Limit
							</label>
							<input
								id={`page-limit-${attributeName}`}
								type='number'
								min={minPageLimit}
								value={pageLimit && pageLimit > 0 ? pageLimit : ''}
								onChange={handlePageLimitInputChange}
								onKeyDown={handlePageLimitKeyDown}
								aria-label={`Page limit (minimum ${minPageLimit} for this card)`}
							/>
						</div>
					</div>
				</div>
			</div>
		</div>
	)
}

export default RelationshipCard
