/*
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
    'hbs!tmpl/tag/AddTimezoneView_tmpl',
    'moment',
    'moment-timezone',
    'daterangepicker'
], function(require, Backbone, AddTimezoneViewTmpl, moment) {
    'use strict';

    return Backbone.Marionette.ItemView.extend(
        /** @lends GlobalExclusionListView */
        {

            template: AddTimezoneViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                close: "[data-id='close']",
                timeInterval: 'input[name="timeInterval"]',
                timeZone: '[data-id="timeZone"]'
            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["change " + this.ui.timeInterval] = function(e) {
                    this.buttonActive({ isButtonActive: true });
                };
                events["change " + this.ui.timeZone] = function(e) {
                    this.model.set({ "timeZone": this.ui.timeZone.val() });
                    this.buttonActive({ isButtonActive: true });
                };
                events["click " + this.ui.close] = 'onCloseButton';
                return events;
            },

            /**
             * intialize a new GlobalExclusionComponentView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'parentView', 'model', 'tagModel'));
            },
            onRender: function() {
                var that = this,
                    minDate = new Date(),
                    dateObj = {
                        "showDropdowns": true,
                        "timePicker": true,
                        "timePicker24Hour": true,
                        "startDate": new Date(),
                        "locale": {
                            format: 'YYYY/MM/DD h:mm A'
                        },
                        "alwaysShowCalendars": true
                    },
                    tzstr = '<option selected="selected" disabled="disabled">-- Select Timezone --</option>';

                if (this.model.get('startTime') !== "") {
                    this.ui.timeInterval.daterangepicker({
                        "showDropdowns": true,
                        "timePicker": true,
                        "timePicker24Hour": true,
                        "startDate": this.model.get('startTime'),
                        "endDate": this.model.get('endTime'),
                        "locale": {
                            format: 'YYYY/MM/DD h:mm A'
                        },
                        "alwaysShowCalendars": true
                    }).on('apply.daterangepicker', function(fullDate) {
                        var val = fullDate.currentTarget.value.split(' - ');
                        that.model.set('startTime', val[0]);
                        that.model.set('endTime', val[1]);
                    });
                    this.ui.timeZone.select2({
                        data: moment.tz.names()
                    });
                    this.ui.timeZone.val(this.model.get('timeZone')).trigger("change", { 'manual': true });
                } else {
                    this.ui.timeInterval.daterangepicker(dateObj).on('apply.daterangepicker', function(fullDate) {
                        var val = fullDate.currentTarget.value.split(' - ');
                        that.model.set('startTime', val[0]);
                        that.model.set('endTime', val[1]);
                    });
                    this.ui.timeZone.html(tzstr);
                    this.ui.timeZone.select2({
                        placeholder: "Select TimeZone",
                        allowClear: true,
                        data: moment.tz.names()
                    });
                }
                $('[name="daterangepicker_start"]').attr('readonly', true); +
                $('[name="daterangepicker_end"]').attr('readonly', true);
            },
            buttonActive: function(option) {
                var that = this;
                if (option && option.isButtonActive && that.tagModel) {
                    var isButton = option.isButtonActive;
                    this.parentView.modal.$el.find('button.ok').attr("disabled", isButton === true ? false : true);
                }
            },
            onCloseButton: function() {
                if (this.parentView.collection.models.length > 0) {
                    this.model.destroy();
                }
                if (this.parentView.collection.models.length <= 0) {
                    this.parentView.ui.timeZoneDiv.hide();
                    this.parentView.ui.checkTimeZone.prop('checked', false);
                    this.parentView.modal.$el.find('button.ok').attr("disabled", true);
                }
            }
        });
});