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

define(['require', 'utils/Utils', 'marionette', 'backgrid', 'asBreadcrumbs', 'jquery-placeholder'], function(require, Utils) {
    'use strict';

    Backbone.$.ajaxSetup({
        cache: false
    });

    var oldBackboneSync = Backbone.sync;
    Backbone.sync = function(method, model, options) {
        var that = this;
        return oldBackboneSync.apply(this, [method, model,
            _.extend(options, {
                error: function(response) {
                    if (!options.skipDefaultError) {
                        Utils.defaultErrorHandler(that, response);
                    }
                    that.trigger("error", that, response);
                    if (options.cust_error) {
                        options.cust_error(that, response);
                    }
                }
            })
        ]);
    }
    _.mixin({
        isEmptyArray: function(val) {
            if (val && _.isArray(val)) {
                return _.isEmpty(val);
            } else {
                return false;
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

    // For placeholder support 
    if (!('placeholder' in HTMLInputElement.prototype)) {
        var originalRender = Backbone.Marionette.LayoutView.prototype.render;
        Backbone.Marionette.LayoutView.prototype.render = function() {
            originalRender.apply(this, arguments);
            this.$('input, textarea').placeholder();
        }
    }

    String.prototype.trunc = String.prototype.trunc ||
        function(n) {
            return (this.length > n) ? this.substr(0, n - 1) + '...' : this;
        };
    String.prototype.capitalize = function() {
        return this.charAt(0).toUpperCase() + this.slice(1);
    }



    /*
     * Overriding Cell for adding custom className to Cell i.e <td>
     */
    var cellInit = Backgrid.Cell.prototype.initialize;
    Backgrid.Cell.prototype.initialize = function() {
        cellInit.apply(this, arguments);
        var className = this.column.get('className');
        var rowClassName = this.column.get('rowClassName');
        if (rowClassName) this.$el.addClass(rowClassName);
        if (className) this.$el.addClass(className);
    }
    /*
     * Overriding Cell for adding custom width to Cell i.e <td>
     */
    Backgrid.HeaderRow = Backgrid.HeaderRow.extend({
        render: function() {
            var that = this;
            Backgrid.HeaderRow.__super__.render.apply(this, arguments);
            _.each(this.columns.models, function(modelValue) {
                var elAttr = modelValue.get('elAttr'),
                    elAttrObj = null;
                if (elAttr) {
                    if (_.isFunction(elAttr)) {
                        elAttrObj = elAttr(modelValue);
                    } else if (_.isObject(elAttr)) {
                        if (!_.isArray(elAttr)) {
                            elAttrObj = [elAttr];
                        } else {
                            elAttrObj = elAttr;
                        }
                    }
                    _.each(elAttrObj, function(val) {
                        that.$el.find('.' + modelValue.get('name')).data(val);
                    });
                }
                if (modelValue.get('width')) that.$el.find('.' + modelValue.get('name')).css('min-width', modelValue.get('width') + 'px');
                if (modelValue.get('toolTip')) that.$el.find('.' + modelValue.get('name')).attr('title', modelValue.get('toolTip'));
            });
            return this;
        }
    });
    /*
     * HtmlCell renders any html code
     * @class Backgrid.HtmlCell
     * @extends Backgrid.Cell
     */
    var HtmlCell = Backgrid.HtmlCell = Backgrid.Cell.extend({

        /** @property */
        className: "html-cell",

        render: function() {
            this.$el.empty();
            var rawValue = this.model.get(this.column.get("name"));
            var formattedValue = this.formatter.fromRaw(rawValue, this.model);
            this.$el.append(formattedValue);
            this.delegateEvents();
            return this;
        }
    });


    /*
     * Backgrid Header render listener when resize or re-ordered
     */
    var BackgridHeaderInitializeMethod = function(options) {
        this.columns = options.columns;
        if (!(this.columns instanceof Backbone.Collection)) {
            this.columns = new Backgrid.Columns(this.columns);
        }
        this.createHeaderRow();

        this.listenTo(this.columns, "sort", _.bind(function() {
            this.createHeaderRow();
            this.render();
        }, this));
    };

    /**
     * Sets up a new headerRow and attaches it to the view
     * Tested with backgrid 0.3.5
     */
    var BackgridHeaderCreateHeaderRowMethod = function() {
        this.row = new Backgrid.HeaderRow({
            columns: this.columns,
            collection: this.collection
        });
    };

    /**
     * Tested with backgrid 0.3.5
     */
    var BackgridHeaderRenderMethod = function() {
        this.$el.empty();
        this.$el.append(this.row.render().$el);
        this.delegateEvents();

        // Trigger event
        this.trigger("backgrid:header:rendered", this);

        return this;
    };

    // Backgrid patch
    Backgrid.Header.prototype.initialize = BackgridHeaderInitializeMethod;
    Backgrid.Header.prototype.createHeaderRow = BackgridHeaderCreateHeaderRowMethod;
    Backgrid.Header.prototype.render = BackgridHeaderRenderMethod;

    /* End: Backgrid Header render listener when resize or re-ordered */

    var UriCell = Backgrid.UriCell = Backgrid.Cell.extend({
        className: "uri-cell",
        title: null,
        target: "_blank",

        initialize: function(options) {
            UriCell.__super__.initialize.apply(this, arguments);
            this.title = options.title || this.title;
            this.target = options.target || this.target;
        },

        render: function() {
            this.$el.empty();
            var rawValue = this.model.get(this.column.get("name"));
            var href = _.isFunction(this.column.get("href")) ? this.column.get('href')(this.model) : this.column.get('href');
            var klass = this.column.get("klass");
            var formattedValue = this.formatter.fromRaw(rawValue, this.model);
            this.$el.append($("<a>", {
                tabIndex: -1,
                href: href,
                title: this.title || formattedValue,
                'class': klass
            }).text(formattedValue));

            if (this.column.has("iconKlass")) {
                var iconKlass = this.column.get("iconKlass");
                var iconTitle = this.column.get("iconTitle");
                this.$el.find('a').append('<i class="' + iconKlass + '" title="' + iconTitle + '"></i>');
            }
            this.delegateEvents();
            return this;
        }
    });
});