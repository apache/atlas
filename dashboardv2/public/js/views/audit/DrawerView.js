/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define(['require',
    'backbone',
    'hbs!tmpl/audit/DrawerView_tmpl',
    'utils/Utils'
], function (require, Backbone, DrawerView_tmpl, Utils) {
    'use strict';

    var DrawerView = Backbone.Marionette.LayoutView.extend({
        _viewName: 'DrawerView',
        template: DrawerView_tmpl,

        ui: {
            overlay: ".drawer-overlay",
            panel: ".drawer-panel",
            closeBtn: "[data-id='closeDrawer']",
            searchInput: "[data-id='searchInput']",
            drawerList: "[data-id='drawerList']",
            drawerItemsList: "[data-id='drawerList']",
            drawerScrollRegion: "[data-id='drawerScrollRegion']",
            drawerEmpty: "[data-id='drawerEmpty']",
            limitInput: "[data-id='limitInput']",
            showingText: "[data-id='showingText']",
            copyRunId: "[data-id='copyRunId']",
            drawerLoading: "[data-id='drawerLoading']",
            drawerObserver: "[data-id='drawerObserver']"
        },

        events: function () {
            var events = {};
            events["click " + this.ui.closeBtn] = "closeDrawer";
            events["click " + this.ui.overlay] = "closeDrawer";
            events["keyup " + this.ui.searchInput] = "onSearch";
            events["keyup " + this.ui.limitInput] = function (e) {
                if (e.keyCode === 13) {
                    this.onLimitChange();
                }
            };
            events["click .blue-link"] = "onItemClick";
            events["click " + this.ui.copyRunId] = "onCopyRunId";
            return events;
        },

        initialize: function (options) {
            _.extend(this, _.pick(options, 'title', 'items', 'fetchData', 'onItemClickCb', 'actionType', 'runId', 'totalCount'));
            this.limit = 10;
            this.searchText = "";
            this.displayItems = [];
            this.offset = 0;
            this.page = 1;
            this.hasMore = true;
            this.isLoading = false;
            this.scrollTop = 0;
        },

        onRender: function () {
            var that = this;
            $('body').append(this.$el);

            setTimeout(function () {
                that.ui.overlay.addClass('open');
                that.ui.panel.addClass('open');
            }, 10);

            // Scroll events don't bubble, so we must bind directly to the UI element
            this.ui.drawerScrollRegion.on('scroll', function () {
                var el = this;
                that.scrollTop = el.scrollTop;
                that.renderList(); // re-render the visible chunk

                var isNearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 20;
                if (isNearBottom && that.hasMore && !that.isLoading) {
                    if (that.scrollTimer) clearTimeout(that.scrollTimer);
                    that.scrollTimer = setTimeout(function () {
                        that.loadMore();
                    }, 250);
                }
            });

            // Only use IntersectionObserver for requested entities to preserve existing behavior
            if (this.actionType !== 'purged') {
                this.setupObserver();
            }

            this.loadData();
        },

        setupObserver: function () {
            var that = this;
            if (this.observer) {
                this.observer.disconnect();
            }

            // Using IntersectionObserver is more reliable than manual scroll math
            this.observer = new IntersectionObserver(function (entries) {
                if (entries[0].isIntersecting && that.hasMore && !that.isLoading) {
                    that.loadMore();
                }
            }, {
                root: this.ui.drawerScrollRegion[0],
                rootMargin: '0px',
                threshold: 0.1
            });

            if (this.ui.drawerObserver.length) {
                this.observer.observe(this.ui.drawerObserver[0]);
            }
        },

        onBeforeDestroy: function () {
            if (this.observer) {
                this.observer.disconnect();
            }
            if (this.scrollTimer) {
                clearTimeout(this.scrollTimer);
            }
            if (this.ui && this.ui.drawerScrollRegion) {
                this.ui.drawerScrollRegion.off('scroll');
            }
        },

        loadData: function () {
            this.offset = 0;
            this.page = 1;
            this.hasMore = true;
            this.displayItems = [];

            if (this.fetchData) {
                // Server-side: paginate via API calls
                this.items = [];
            }
            this.loadMore();
        },

        loadMore: function () {
            var that = this;
            if (this.isLoading || !this.hasMore) return;

            this.isLoading = true;
            this.ui.drawerLoading.show(); // Show loading indicator

            if (this.fetchData) {
                // Server-side pagination via API
                this.fetchData(this.limit, this.offset, function (data) {
                    that.isLoading = false;
                    that.ui.drawerLoading.hide(); // Hide loading indicator

                    if (data && data.length > 0) {
                        that.items = that.items.concat(data);
                        that.offset += data.length;
                        // If we received fewer items than requested, we've likely hit the end
                        if (data.length < that.limit) {
                            that.hasMore = false;
                        }
                    } else {
                        that.hasMore = false;
                    }
                    that.displayItems = that.items;
                    that.renderList();
                });
            } else {
                // Client-side: Paginate and render
                this.isLoading = false;
                this.ui.drawerLoading.hide();
                if (this.displayItems.length > 0) {
                    this.page += 1;
                }
                this.renderList();
            }
        },

        renderList: function () {
            var total = this.totalCount || 0;

            if (!this.fetchData) {
                var fullList = this.items || [];
                if (this.searchText) {
                    var lowerSearch = this.searchText.toLowerCase();
                    fullList = fullList.filter(function (item) {
                        return item.toLowerCase().indexOf(lowerSearch) !== -1;
                    });
                }
                total = fullList.length;
                this.displayItems = fullList.slice(0, this.page * this.limit);
                this.hasMore = this.displayItems.length < fullList.length;
            } else if (this.fetchData && this.searchText) {
                // Server-side search: still need to rely on offset/limit
                // Re-using existing logic
                var fullList = this.items || [];
                var lowerSearch = this.searchText.toLowerCase();
                fullList = fullList.filter(function (item) {
                    return item.toLowerCase().indexOf(lowerSearch) !== -1;
                });
                this.displayItems = fullList.slice(0, Math.min(this.offset, fullList.length));
            }

            var listHtml = "";

            var virtualizedData = Utils.virtualizeList({
                items: this.displayItems,
                scrollTop: this.scrollTop,
                itemHeight: 37,
                overscan: 10,
                visibleCount: 40
            });
            
            if (virtualizedData.paddingTop > 0) {
                listHtml += '<div style="height: ' + virtualizedData.paddingTop + 'px"></div>';
            }

            _.each(virtualizedData.visibleItems, function (item, index) {
                var globalIndex = virtualizedData.startIndex + index + 1;
                listHtml += '<li class="drawer-list-item" style="height: 37px; box-sizing: border-box;"><span class="item-index">' + globalIndex + '.</span> <a class="blue-link" title="' + _.escape(item) + '" data-guid="' + _.escape(item) + '">' + _.escape(item) + '</a></li>';
            });
            
            if (virtualizedData.paddingBottom > 0) {
                listHtml += '<div style="height: ' + virtualizedData.paddingBottom + 'px"></div>';
            }

            this.ui.drawerItemsList.html(listHtml);

            // Show 'Scroll to load more'
            if (this.hasMore && this.displayItems.length > 0) {
                this.ui.drawerItemsList.append('<li class="drawer-load-more">Scroll to load more data</li>');
            }

            this.ui.drawerLoading.hide();

            if (this.displayItems.length === 0) {
                this.ui.drawerEmpty.show();
            } else {
                this.ui.drawerEmpty.hide();
            }

            var showing = this.displayItems.length;
            if (!this.fetchData) {
                this.ui.showingText.text("Showing " + showing + " of " + total);
            } else {
                this.ui.showingText.text("Showing " + showing + " of " + total);
            }
        },

        onSearch: function (e) {
            this.searchText = $(e.currentTarget).val().trim();
            this.scrollTop = 0;
            this.loadData();
        },

        onLimitChange: function () {
            var newLimit = parseInt(this.ui.limitInput.val(), 10);
            var maxLimit = this.totalCount || 0;
            if (newLimit > 0) {
                if (maxLimit > 0) {
                    newLimit = Math.min(newLimit, maxLimit);
                    this.ui.limitInput.val(newLimit);
                }
                this.limit = newLimit;
                this.scrollTop = 0;
                this.loadData();
            }
        },

        onItemClick: function (e) {
            var guid = $(e.currentTarget).data('guid');
            if (this.onItemClickCb) {
                this.onItemClickCb(guid, this.actionType);
            }
        },

        onCopyRunId: function () {
            var $temp = $("<input>");
            $("body").append($temp);
            $temp.val(this.runId).select();
            document.execCommand("copy");
            $temp.remove();
            Utils.notifySuccess({
                content: "Run Id copied to clipboard"
            });
        },

        closeDrawer: function () {
            var that = this;
            this.ui.overlay.removeClass('open');
            this.ui.panel.removeClass('open');

            setTimeout(function () {
                that.destroy();
            }, 300);
        },

        templateHelpers: function () {
            return {
                title: this.title || 'Entities',
                limit: this.limit,
                runId: this.runId,
                fetchData: this.fetchData
            };
        }
    });

    return DrawerView;
});
