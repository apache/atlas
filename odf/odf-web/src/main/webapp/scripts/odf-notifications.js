/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var $ = require("jquery");
var React = require("react");
var ReactDOM = require("react-dom");
var d3 = require("d3");
var ReactBootstrap = require("react-bootstrap");
var ReactD3 = require("react-d3-components");
var ODFGlobals = require("./odf-globals.js");
var AJAXCleanupMixin = require("./odf-mixins.js");
var ReactD3 = require("react-d3-components");
var LineChart = ReactD3.LineChart;
var Input = ReactBootstrap.Input;
var Image = ReactBootstrap.Image;

var REFRESH_DELAY = 5000;

var CurrentNotificationsGraph = React.createClass({

	tooltipLine : function(label, data) {
        return "Arrived notifications " + data.y;
    },

	render : function(){
		var lineChart = null;

		if(this.props.values){
			var data = [
			        {
			        	label: 'Asset notifications',
			            values: [ ]
			         }
			    ];

			for(var no = 0; no < this.props.values.length; no++){
				data[0].values.push({x : no + 1, y : this.props.values[no]});
			};

			lineChart = (<LineChart
			                data={data}
							width={400}
			                height={400}
			                margin={{top: 10, bottom: 50, left: 50, right: 10}}
			                tooltipContained
		                    tooltipHtml={this.tooltipLine}
			                shapeColor={"red"}
			 				xAxis={{tickValues: []}}
							/>);
		}

		return (
				<div>
					<h4>Number of received notifications</h4>
					<h5>(This only works for the node this web application is running on.  In a clustered environment, notifications could be processed on another node and therefore not be visible here)</h5>

					{lineChart}
				</div>);
	}


});

var ODFNotificationsGraph = React.createClass({
	mixins : [AJAXCleanupMixin],

	getInitialState : function(){
		return {notifications : [], notificationCount : [0]};
	},

	getNotifications : function(){
		const url = ODFGlobals.metadataUrl + "/notifications?numberOfNotifications=50";
        var req = $.ajax({
            url: url,
            contentType: "application/json",
            type: 'GET',
            success: function(data) {
            	this.setState({notifications: data.notifications});
            }.bind(this),
            error: function(xhr, status, err) {
              var msg = "ODF notification request failed, " + err.toString();
              this.props.alertCallback({type: "danger", message: msg});
            }.bind(this)
        });

        this.storeAbort(req.abort);
	},

	getNotificationCount : function() {
		const url = ODFGlobals.metadataUrl + "/notifications/count";
        var req = $.ajax({
            url: url,
            contentType: "application/json",
            type: 'GET',
            success: function(data) {
            	var current = this.state.notificationCount;
            	if(!current){
            		current = [];
            	}else if(current.length > 1 && current[current.length - 1] != current[current.length - 2]){
            		this.getNotifications();
            	}
            	if(current.length == 10){
            		current.splice(0, 1);
            	}
            	current.push(data.notificationCount);
               this.setState({notificationCount: current});
            }.bind(this),
            error: function(xhr, status, err) {
              var msg = "ODF notification count request failed, " + err.toString();
              this.props.alertCallback({type: "danger", message: msg});
            }.bind(this)
        });

        this.storeAbort(req.abort);
	},

	componentWillMount : function() {
		this.getNotifications();
		this.getNotificationCount();
	},

	componentWillUnmount () {
	    this.refreshInterval && clearInterval(this.refreshInterval);
	    this.refreshInterval = false;
	},

	componentWillReceiveProps: function(nextProps){
		if(!nextProps.visible){
			 this.refreshInterval && clearInterval(this.refreshInterval);
			 this.refreshInterval = false;
		}else if(!this.refreshInterval){
			this.refreshInterval = window.setInterval(this.getNotificationCount, REFRESH_DELAY);
		}
	},
	render : function(){
		var progressIndicator = <Image src="img/lg_proc.gif" rounded />;

		var notificationGraph = null;
		if(this.state){
			progressIndicator = null;
			notificationGraph = <CurrentNotificationsGraph values={this.state.notificationCount} />;
		}

		var notificationsValue = "";
		$.each(this.state.notifications, function(key, val){
			notificationsValue +="\n";
			notificationsValue += val.type + " , " + val.asset.repositoryId + " -- " + val.asset.id;
		});

		return (
				<div>
					{progressIndicator}
					{notificationGraph}
					<textarea disabled style={{width: '100%', height: '300px'}} value={notificationsValue} />
				</div>);

	}
});

module.exports = ODFNotificationsGraph;
