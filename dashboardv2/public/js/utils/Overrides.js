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

define(['require', 'backgrid', 'asBreadcrumbs'], function(require) {
    'use strict';
    $.asBreadcrumbs.prototype.generateChildrenInfo = function() {
        var self = this;
        this.$children.each(function() {
            var $this = $(this);
            self.childrenInfo.push({
                $this: $this,
                outerWidth: $this.outerWidth(),
                $content: $(self.options.dropdownContent($this))
            });
        });
        if (this.options.overflow === "left") {
            this.childrenInfo.reverse();
        }
        this.childrenLength = this.childrenInfo.length;
    };
    String.prototype.trunc = String.prototype.trunc ||
        function(n) {
            return (this.length > n) ? this.substr(0, n - 1) + '...' : this;
        };
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
