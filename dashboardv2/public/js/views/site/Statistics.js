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
    'hbs!tmpl/site/Statistics_tmpl',
    'hbs!tmpl/site/Statistics_Notification_table_tmpl',
    'modules/Modal',
    'models/VCommon',
    'utils/UrlLinks',
    'collection/VTagList',
    'utils/CommonViewFunction',
    'utils/Enums',
    'moment',
    'utils/Utils',
    'moment-timezone'
], function(require, Backbone, StatTmpl, StatsNotiTable, Modal, VCommon, UrlLinks, VTagList, CommonViewFunction, Enums, moment, Utils) {
    'use strict';

    var StatisticsView = Backbone.Marionette.LayoutView.extend(
        /** @lends AboutAtlasView */
        {
            template: StatTmpl,

            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                entityActive: "[data-id='entityActive'] tbody",
                entityDelete: "[data-id='entityDelete'] tbody",
                entityActiveHeader: "[data-id='entityActive'] .count",
                entityDeletedHeader: "[data-id='entityDelete'] .count",
                serverCard: "[data-id='server-card']",
                connectionCard: "[data-id='connection-card']",
                notificationCard: "[data-id='notification-card']",
                statsNotificationTable: "[data-id='stats-notification-table']",
                notificationSmallCard: "[data-id='notification-small-card']"
            },
            /** ui events hash */
            events: function() {},
            /**
             * intialize a new AboutAtlasView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, options);
                var modal = new Modal({
                    title: 'Statistics',
                    content: this,
                    okCloses: true,
                    showFooter: true,
                    allowCancel: false,
                    width: "60%"
                }).open();

                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            bindEvents: function() {},
            onRender: function() {
                var that = this;
                var entityCountCollection = new VTagList();
                entityCountCollection.url = UrlLinks.entityCountApi();
                entityCountCollection.modelAttrName = "data";
                entityCountCollection.fetch({
                    success: function(data) {
                        var data = _.first(data.toJSON()),
                            no_records = '<tr class="empty text-center"><td colspan="2"><span>No records found!</span></td></tr>',
                            activeEntityTable = _.isEmpty(data.entity.entityActive) ? no_records : that.getTable({ valueObject: data.entity.entityActive }),
                            deleteEntityTable = _.isEmpty(data.entity.entityDeleted) ? no_records : that.getTable({ valueObject: data.entity.entityDeleted });
                        that.renderStats({ valueObject: data.general.stats });
                        var totalActive = 0,
                            totalDeleted = 0;
                        if (data.entity && data.general.entityCount) {
                            totalActive = data.general.entityCount;
                        }
                        if (data.entity && data.entity.entityDeleted) {
                            _.each(data.entity.entityDeleted, function(val) {
                                totalDeleted += val;
                            });
                        }
                        that.ui.entityActive.html(activeEntityTable);
                        that.ui.entityDelete.html(deleteEntityTable);
                        that.ui.entityActiveHeader.html("&nbsp;(" + _.numberFormatWithComa((totalActive - totalDeleted)) + ")");
                        that.ui.entityDeletedHeader.html("&nbsp;(" + _.numberFormatWithComa(totalDeleted) + ")");
                        that.$('.statsContainer,.statsNotificationContainer').removeClass('hide');
                        that.$('.statsLoader,.statsNotificationLoader').removeClass('show');
                    }
                });
            },
            genrateStatusData: function(stateObject) {
                var that = this,
                    stats = {};
                _.each(stateObject, function(val, key) {
                    var keys = key.split(":"),
                        key = keys[0],
                        subKey = keys[1];
                    if (stats[key]) {
                        stats[key][subKey] = val;
                    } else {
                        stats[key] = {};
                        stats[key][subKey] = val;
                    }
                });
                return stats;
            },
            renderStats: function(options) {
                var that = this,
                    data = this.genrateStatusData(options.valueObject),
                    createTable = function(obj) {
                        var tableBody = '',
                            enums = obj.enums,
                            data = obj.data,
                            showConnectionStatus = obj.showConnectionStatus;
                        _.each(data, function(value, key, list) {
                            tableBody += '<tr><td>' + key + '</td><td class="">' + that.getValue({
                                "value": value,
                                "type": enums[key],
                                "showConnectionStatus": showConnectionStatus
                            }) + '</td></tr>';
                        });
                        return tableBody;
                    };
                if (data.Notification) {
                    var tableCol = [{ label: "Total", key: "total" },
                            {
                                label: "Current Hour <br> (from " + (that.getValue({
                                    "value": data.Notification["currentHourStartTime"],
                                    "type": Enums.stats.Notification["currentHourStartTime"],
                                })) + ")",
                                key: "currentHour"
                            },
                            { label: "Previous Hour", key: "previousHour" },
                            {
                                label: "Current Day <br> (from " + (that.getValue({
                                    "value": data.Notification["currentDayStartTime"],
                                    "type": Enums.stats.Notification["currentDayStartTime"],
                                })) + ")",
                                key: "currentDay"
                            },
                            { label: "Previous Day", key: "previousDay" }
                        ],
                        tableHeader = ["count", "AvgTime", "EntityCreates", "EntityUpdates", "EntityDeletes", "Failed"];
                    that.ui.notificationCard.html(
                        StatsNotiTable({
                            "enums": Enums.stats.Notification,
                            "data": data.Notification,
                            "tableHeader": tableHeader,
                            "tableCol": tableCol,
                            "getValue": function(argument, args) {
                                var returnVal = (args == 'count' ? data.Notification[argument.key] : data.Notification[argument.key.concat(args)]);
                                return returnVal ? _.numberFormatWithComa(returnVal) : 0;
                            }
                        })
                    );

                    that.ui.notificationSmallCard.html(createTable({
                        "enums": Enums.stats.Notification,
                        "data": _.pick(data.Notification, 'lastMessageProcessedTime', 'offsetCurrent', 'offsetStart')
                    }));
                }

                if (data.Server) {
                    that.ui.serverCard.html(
                        createTable({
                            "enums": _.extend(Enums.stats.Server, Enums.stats.ConnectionStatus),
                            "data": _.extend(_.pick(data.Server, 'startTimeStamp', 'activeTimeStamp', 'upTime'), data.ConnectionStatus),
                            "showConnectionStatus": true
                        })
                    );
                }
            },
            getValue: function(options) {
                var value = options.value,
                    type = options.type,
                    showConnectionStatus = options.showConnectionStatus;
                if (type == 'time') {
                    return Utils.millisecondsToTime(value);
                } else if (type == 'day') {
                    return moment.tz(value, moment.tz.guess()).format("MM/DD/YYYY h:mm A z");
                } else if (type == 'number') {
                    return _.numberFormatWithComa(value);
                } else if (type == 'millisecond') {
                    return _.numberFormatWithComa(value) + " millisecond/s";
                } else if ((showConnectionStatus && (value.indexOf('connected') != -1))) {
                    return '<span class="connection-status ' + (showConnectionStatus && showConnectionStatus == true ? value : "") + '"></span>';
                } else {
                    return value;
                }
            },
            getTable: function(obj) {
                return CommonViewFunction.propertyTable(_.extend({
                    scope: this,
                    formatIntVal: true
                }, obj))
            }
        });
    return StatisticsView;
});