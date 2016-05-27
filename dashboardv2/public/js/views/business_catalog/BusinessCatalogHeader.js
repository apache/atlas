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
    'hbs!tmpl/business_catalog/BusinessCatalogHeader'
], function(require, tmpl) {
    'use strict';

    var BusinessCatalogHeader = Marionette.LayoutView.extend({
        template: tmpl,
        templateHelpers: function() {},
        regions: {},
        events: {},
        initialize: function(options) {
            _.extend(this, _.pick(options, 'globalVent', 'url', 'collection'));
            this.value = [];

        },
        /**
         * After Page Render createBrudCrum called.
         * @return {[type]} [description]
         */
        render: function() {
            $(this.el).html(this.template());
            var that = this;
            if (this.url) {
                var t = [];
                var splitURL = this.url.split("api/atlas/v1/taxonomies/");
                if (splitURL.length > 1) {
                    var x = splitURL[1].split("/terms/");
                }

                var href = "";
                for (var v in x) {
                    if (v == 0) {
                        href = x[v];
                        t.push({
                            value: x[v],
                            href: href
                        });
                    } else {
                        href += "/terms/" + x[v];
                        t.push({
                            value: x[v],
                            href: href
                        })
                    };
                }
                this.value = t;
            }
            this.listenTo(this.collection, 'reset', function() {
                setTimeout(function() {
                    that.createBrudCrum();
                }, 0);

            }, this);
            return this;
        },
        createBrudCrum: function() {
            var li = "",
                value = this.value,
                that = this;
            _.each(value, function(object) {
                li += '<li><a href="/#!/taxonomy/detailCatalog/api/atlas/v1/taxonomies/' + object.href + '?back=true">' + object.value + '</a></li>';
            });
            this.$('.breadcrumb').html(li);
            //this.$(".breadcrumb").asBreadcrumbs("destroy");
            this.$('.breadcrumb').asBreadcrumbs({
                namespace: 'breadcrumb',
                overflow: "left",
                dropicon: "fa fa-ellipsis-h",
                dropdown: function() {
                    return '<div class=\"dropdown\">' +
                        '<a href=\"javascript:void(0);\" class=\"' + this.namespace + '-toggle\" data-toggle=\"dropdown\"><i class=\"' + this.dropicon + '\"</i></a>' +
                        '<ul class=\"' + this.namespace + '-menu dropdown-menu popover bottom arrowPosition \" ><div class="arrow"></div></ul>' +
                        '</div>';
                },
                dropdownContent: function(a) {
                    return '<li><a href="' + a.find('a').attr('href') + '" class="dropdown-item">' + a.text() + "</a></li>";
                }
            });
        }
    });
    return BusinessCatalogHeader;
});
