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
    'hbs!tmpl/business_catalog/BusinessCatalogHeader',
    'utils/CommonViewFunction',
    'utils/Globals'
], function(require, tmpl, CommonViewFunction, Globals) {
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
            var that = this;
            $(this.el).html(this.template());
            if (Globals.userLogedIn.status) {
                that.$('.userName').html(Globals.userLogedIn.response.userName);
            }
            var that = this;
            if (this.url) {
                this.value = CommonViewFunction.breadcrumbUrlMaker(this.url);
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
            CommonViewFunction.breadcrumbMaker({ urlList: this.value, scope: this.$('.breadcrumb') });
        }
    });
    return BusinessCatalogHeader;
});
