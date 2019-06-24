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
        if (options.queryParam) {
            var generateQueryParam = $.param(options.queryParam);
            if (options.url.indexOf('?') !== -1) {
                options.url = options.url + "&" + generateQueryParam;
            } else {
                options.url = options.url + "?" + generateQueryParam;
            }
        }
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