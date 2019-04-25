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
    'utils/Utils',
    'd3',
    'marionette'
], function(require, Utils, d3) {
    'use strict';
    _.mixin({
        numberFormatWithComa: function(number) {
            return d3.format(',')(number);
        },
        isEmptyArray: function(val) {
            if (val && _.isArray(val)) {
                return _.isEmpty(val);
            } else {
                return false;
            }
        },
        toArrayifObject: function(val) {
            return _.isObject(val) ? [val] : val;
        },
        startsWith: function(str, matchStr) {
            if (str && matchStr && _.isString(str) && _.isString(matchStr)) {
                return str.lastIndexOf(matchStr, 0) === 0
            } else {
                return;
            }
        },
        isUndefinedNull: function(val) {
            if (_.isUndefined(val) || _.isNull(val)) {
                return true
            } else {
                return false;
            }
        },
        trim: function(val) {
            if (val && val.trim) {
                return val.trim();
            } else {
                return val;
            }
        },
        isTypePrimitive: function(type) {
            if (type === "int" || type === "byte" || type === "short" || type === "long" || type === "float" || type === "double" || type === "string" || type === "boolean" || type === "date") {
                return true;
            }
            return false;
        }
    });
    var getPopoverEl = function(e) {
        return $(e.target).parent().data("bs.popover") || $(e.target).data("bs.popover") || $(e.target).parents('.popover').length;
    }
    $(document).on('click DOMMouseScroll mousewheel', function(e) {
        if (e.originalEvent) {
            // Do action if it is triggered by a human.
            //e.isImmediatePropagationStopped();
            var isPopOverEl = getPopoverEl(e)
            if (!isPopOverEl) {
                $('.popover').popover('hide');
            } else if (isPopOverEl.$tip) {
                $('.popover').not(isPopOverEl.$tip).popover('hide');
            }
        }
    });
    $('body').on('hidden.bs.popover', function(e) {
        $(e.target).data("bs.popover").inState = { click: false, hover: false, focus: false }
    });
    $('body').on('show.bs.popover', '[data-js="popover"]', function() {
        $('.popover').not(this).popover('hide');
    });
    $('body').on('keypress', 'input.number-input,.number-input .select2-search__field', function(e) {
        if (e.which != 8 && e.which != 0 && (e.which < 48 || e.which > 57)) {
            return false;
        }
    });
    $('body').on('keypress', 'input.number-input-negative,.number-input-negative .select2-search__field', function(e) {
        if (e.which != 8 && e.which != 0 && (e.which < 48 || e.which > 57)) {
            if (e.which == 45) {
                if (this.value.length) {
                    return false;
                }
            } else {
                return false;
            }
        }
    });
    $('body').on('keypress', 'input.number-input-exponential,.number-input-exponential .select2-search__field', function(e) {
        if ((e.which != 8 && e.which != 0) && (e.which < 48 || e.which > 57) && (e.which != 69 && e.which != 101 && e.which != 43 && e.which != 45 && e.which != 46 && e.which != 190)) {
            return false;
        }
    });
    $("body").on('click', '.dropdown-menu.dropdown-changetitle li a', function() {
        $(this).parents('li').find(".btn:first-child").html($(this).text() + ' <span class="caret"></span>');
    });
    $("body").on('click', '.btn', function() {
        $(this).blur();
    });

    $.fn.select2.amd.define("ServiceTypeFilterDropdownAdapter", [
            "select2/utils",
            "select2/dropdown",
            "select2/dropdown/attachBody",
            "select2/dropdown/attachContainer",
            "select2/dropdown/search",
            "select2/dropdown/minimumResultsForSearch",
            "select2/dropdown/closeOnSelect",
        ],
        function(Utils, Dropdown, AttachBody, AttachContainer, Search, MinimumResultsForSearch, CloseOnSelect) {

            // Decorate Dropdown with Search functionalities
            var dropdownWithSearch = Utils.Decorate(Utils.Decorate(Dropdown, CloseOnSelect), Search);

            dropdownWithSearch.prototype.render = function() {
                // Copy and modify default search render method
                var $rendered = Dropdown.prototype.render.call(this);

                // Add ability for a placeholder in the search box
                var placeholder = this.options.get("placeholderForSearch") || "";
                var $search = $(
                    '<span class="select2-search select2-search--dropdown"><div class="row">' +
                    '<div class="col-md-10"><input class="select2-search__field" placeholder="' + placeholder + '" type="search"' +
                    ' tabindex="-1" autocomplete="off" autocorrect="off" autocapitalize="off"' +
                    ' spellcheck="false" role="textbox" /></div>' +
                    '<div class="col-md-2"><button type="button" style="margin-left: -20px" class="btn btn-action btn-sm filter " title="Type Filter"><i class="fa fa-filter"></i></button></div>' +
                    '</div></span>'
                );
                if (!this.options.options.getFilterBox) {
                    throw "In order to render the filter options adapter needed getFilterBox function"
                }
                var $Filter = $('<ul class="type-filter-ul"></ul>');
                this.$Filter = $Filter;
                this.$Filter.append(this.options.options.getFilterBox());
                this.$Filter.hide();

                this.$searchContainer = $search;
                if ($Filter.find('input[type="checkbox"]:checked').length) {
                    $search.find('button.filter').addClass('active');
                } else {
                    $search.find('button.filter').removeClass('active');
                }
                this.$search = $search.find('input');

                $rendered.prepend($search);
                $rendered.append($Filter);
                return $rendered;
            };
            var oldDropdownWithSearchBindRef = dropdownWithSearch.prototype.bind;
            dropdownWithSearch.prototype.bind = function(container, $container) {
                var self = this;
                oldDropdownWithSearchBindRef.call(this, container, $container);
                var self = this;
                this.$Filter.on('click', 'li', function() {
                    var itemCallback = self.options.options.onFilterItemSelect;
                    itemCallback && itemCallback(this);
                })

                this.$searchContainer.find('button.filter').click(function() {
                    container.$dropdown.find('.select2-search').hide(150);
                    container.$dropdown.find('.select2-results').hide(150);
                    self.$Filter.html(self.options.options.getFilterBox());
                    self.$Filter.show();
                });
                this.$Filter.on('click', 'button.filterDone', function() {
                    container.$dropdown.find('.select2-search').show(150);
                    container.$dropdown.find('.select2-results').show(150);
                    self.$Filter.hide();
                    var filterSubmitCallback = self.options.options.onFilterSubmit;
                    filterSubmitCallback && filterSubmitCallback({
                        filterVal: _.map(self.$Filter.find('input[type="checkbox"]:checked'), function(item) {
                            return $(item).data('value')
                        })
                    });
                });
                container.$element.on('hideFilter', function() {
                    container.$dropdown.find('.select2-search').show();
                    container.$dropdown.find('.select2-results').show();
                    self.$Filter.hide();
                });

            }
            // Decorate the dropdown+search with necessary containers
            var adapter = Utils.Decorate(dropdownWithSearch, AttachContainer);
            adapter = Utils.Decorate(adapter, AttachBody);

            return adapter;
        });

    // For placeholder support 
    if (!('placeholder' in HTMLInputElement.prototype)) {
        var originalRender = Backbone.Marionette.LayoutView.prototype.render;
        Backbone.Marionette.LayoutView.prototype.render = function() {
            originalRender.apply(this, arguments);
            this.$('input, textarea').placeholder();
        }
    }
    $('body').on('click', 'pre.code-block .expand-collapse-button', function(e) {
        var $el = $(this).parents('.code-block');
        if ($el.hasClass('shrink')) {
            $el.removeClass('shrink');
        } else {
            $el.addClass('shrink');
        }
    });

    // For adding tooltip globally
    $('body').tooltip({
        selector: '[title]',
        placement: 'bottom',
        container: 'body'
    });

})