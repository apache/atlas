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
var d3 = require("d3");
var ReactBootstrap = require("react-bootstrap");
var ReactD3 = require("react-d3-components");

var AJAXCleanupMixin = require("./odf-mixins.js");

var Image = ReactBootstrap.Image;
var Panel = ReactBootstrap.Panel;
var BarChart = ReactD3.BarChart;
var PieChart = ReactD3.PieChart;
var LineChart = ReactD3.LineChart;

var ODFGlobals = require("./odf-globals.js");

const GRAPH_REFRESH_DELAY_MS = 4000;

var ODFStats = {
	CurrentThreadGraph : React.createClass({

		tooltipLine : function(label, data) {
	        return "Running threads " + data.y;
	    },

	    xScale : function() {
	    	return "";
	    },

		render : function(){
			var lineChart = null;

			if(this.props.threadValues){

				var data = [
				        {
				        	label: 'Thread count',
				            values: [ ]
				         }
				    ];

				for(var no = 0; no < this.props.threadValues.length; no++){
					data[0].values.push({x : no + 1, y : this.props.threadValues[no]});
				};

				lineChart = <LineChart
				                data={data}
								width={400}
				                height={400}
				                margin={{top: 10, bottom: 50, left: 50, right: 10}}
				                tooltipContained
			                    tooltipHtml={this.tooltipLine}
				                shapeColor={"red"}
				 				xAxis={{tickValues: []}}
								/>;
			}

			return (
					<div>
						<h4>Currently running threads in ODF</h4>
						{lineChart}
					</div>);
		}


	}),

	SystemDiagnostics : React.createClass({
		mixins : [AJAXCleanupMixin],

		getODFStatus : function(){
			var currentState = this.state;

			const url = ODFGlobals.engineUrl + "/status";
	        var req = $.ajax({
	            url: url,
	            contentType: "application/json",
	            dataType: 'json',
	            type: 'GET',
	            success: function(data) {
	            	if(currentState == null){
	            		currentState = { threadValues : [0]};
	            	}
	            	currentState.threadValues.push(data.threadManagerStatus.length);
	                if(currentState.threadValues.length > 5){
	                	currentState.threadValues.splice(0, 1);
	                }

	            	this.setState(currentState);
	            }.bind(this),
	            error: function(xhr, status, err) {
	              var msg = "ODF status request failed, " + err.toString();
	              this.props.alertCallback({type: "danger", message: msg});
	            }.bind(this)
	        });

	        this.storeAbort(req.abort);
		},

		componentWillMount : function() {
			this.getODFStatus();
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
				this.refreshInterval = window.setInterval(this.getODFStatus, GRAPH_REFRESH_DELAY_MS);
			}
		},

		tooltipLine : function(label, data) {
	        return "Running threads " + data.y;
	    },

	    xScale : function() {
	    	return "";
	    },

		render : function(){
			var progressIndicator = <Image src="img/lg_proc.gif" rounded />;

			var threadGraph = null;
			if(this.state){
				progressIndicator = null;
				threadGraph = <ODFStats.CurrentThreadGraph threadValues={this.state.threadValues} />;
			}

			return (
					<div>
						{progressIndicator}
						{threadGraph}
					</div> );
		}
	}),

	TotalAnalysisGraph : React.createClass({
		mixins : [AJAXCleanupMixin],

		getAnalysisStats : function() {
			const url = ODFGlobals.analysisUrl + "/stats";
	        var req = $.ajax({
	            url: url,
	            contentType: "application/json",
	            dataType: 'json',
	            type: 'GET',
	            success: function(data) {
	               this.setState(data);
	            }.bind(this),
	            error: function(xhr, status, err) {
	              var msg = "Analysis stats request failed, " + err.toString();
	              this.props.alertCallback({type: "danger", message: msg});
	            }.bind(this)
	        });

	        this.storeAbort(req.abort);
		},

		componentWillMount : function() {
			this.getAnalysisStats();
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
				this.refreshInterval = window.setInterval(this.getAnalysisStats, GRAPH_REFRESH_DELAY_MS);
			}
		},

		tooltipPie : function(x, y) {
		    return y.toString() + " absolute";
		},

		render : function() {
			var progressIndicator = <Image src="img/lg_proc.gif" rounded />;
			var pieChart = null;

			if(this.state){
				progressIndicator = null;
				var succ = (this.state.success ? this.state.success : (this.state.failure ? 0 : 100));
				var fail = (this.state.failure ? this.state.failure : 0);
				var onePercent = (succ + fail) / 100;

				var succVal = (onePercent == 0 ? 100 : (succ / onePercent)).toFixed(2);
				var failVal = (onePercent == 0 ? 0 : (fail / onePercent)).toFixed(2);

				var pieData = {label: "Total success and failure",
								values : [{x: "Finished requests (" + succVal + " %)", y: succ},
								          {x: "Failed requests (" + failVal + " %)", y: fail}
									]
								};

				var colorScale = d3.scale.ordinal().range(["lightgreen", "#F44336"]);

				var pieStyle = {opacity : "1 !important"};
				pieChart = (<PieChart
	                    data={pieData}
	                    width={800}
	                    height={400}
	                    margin={{top: 10, bottom: 10, left: 200, right: 200}}
	                    tooltipHtml={this.tooltipPie}
	                    tooltipOffset={{top: 175, left: 200}}
						tooltipMode={"fixed"}
						style={pieStyle}
   				      	colorScale={colorScale}
	                    />);
			}
			return (
					<div>
						<h4>Total analysis requests and failures</h4>
			           	{progressIndicator}
						{pieChart}
						<hr />
					</div>);
		}
	}),

	PerServiceStatusGraph : React.createClass({
		mixins : [AJAXCleanupMixin],

		getServiceStatus : function() {
			const url = ODFGlobals.servicesUrl + "/status";
	        var req = $.ajax({
	            url: url,
	            contentType: "application/json",
	            dataType: 'json',
	            type: 'GET',
	            success: function(data) {
	               this.setState(data);
	            }.bind(this),
	            error: function(xhr, status, err) {
	              var msg = "Analysis stats request failed, " + err.toString();
	              this.props.alertCallback({type: "danger", message: msg});
	            }.bind(this)
	        });

	        this.storeAbort(req.abort);
		},

		componentWillMount : function() {
			this.getServiceStatus();
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
				this.refreshInterval = window.setInterval(this.getServiceStatus, GRAPH_REFRESH_DELAY_MS);
			}
		},

		tooltip : function(x, y0, y, total) {
			var barData = this.getBarData();
			var text = y;
			var name = null;
			if(barData && barData.length > 0){
				$.map(barData, function(res){
					var bData = barData;
					$.map(res.values, function(val){
						if(val.x == x && val.y == y){
							name = val.fullName;
						}
					});
				});
			}

			var tooltipStyle = {top : "-20px", position: "absolute", left: "-100px", "minWidth" : "350px"};

			if(name == null){
				tooltipStyle.left = 0;
			}

		    return (
		    		<div style={tooltipStyle}>
		    			<span>{name}, {text}</span>
		    		</div>
		    		);
		},

		getBarData : function(){
			if(this.state && !$.isEmptyObject(this.state)){
				var currentState = this.state;
				var statusMap = {};
				$.map(currentState, function(res){
					var states = res.statusCountMap;
					$.each(states, function(state, count){
						var currentArr = statusMap[state];
						if(currentArr === undefined){
							currentArr = [];
						}

						var lbl = (res.name ? res.name : res.id);
						//only shorten names if more than 1 bar is displayed
						if(currentState && Object.keys(currentState) && Object.keys(currentState).length > 1 && lbl && lbl.length > 17){
							lbl = lbl.substring(0, 17) + "..";
						}

						currentArr.push({"x" : lbl, "y": count, "fullName" : res.name});
						statusMap[state] = currentArr;
					});
				});

				var barData = [];

				$.each(statusMap, function(key, val){
					barData.push({"label" : key, "values" : val});
				});

				barData = barData.reverse();
				return barData;
			}else{
				return [ { "label" : "No data available", "values" : [{"x" : "No data availbale", "y" : 0}]}];
			}
		},

		getLegend : function(barData, colors){
			var lbls = [];
			for(var no = 0; no < barData.length; no++){
				lbls.push(<div key={no} ><span style={{color: colors[no]}}>{barData[no].label}</span><br/></div>);
			};

			return (
					<div style={{float:"right"}}>
						{lbls}
					</div>
				);
		},

		render : function() {
			var progressIndicator = <Image src="img/lg_proc.gif" rounded />;
			var barChart = null;

			if(this.state){
				progressIndicator = null;
				var barData = this.getBarData();

				var barStyle = {marginTop: "50px"};

				var barChart = (<BarChart
				  width={400}
		          height={400}
		          margin={{top: 70, bottom: 50, left: 50, right: 10}}
				  tooltipHtml={this.tooltip}
			      tooltipMode={"element"}/>);

				//cancelled, initialized, error, running, in queue, finished
				var colors = ["black", "#F44336", "lightgreen", "blue", "lightblue", "grey"];
				var colorScale = d3.scale.ordinal().range(colors);

				if(barData != null){
					var barWidth = (Object.keys(this.state).length >= 2 ? Object.keys(this.state).length * 200 : 400);

					barChart = (
								<div style={barStyle}>
									{this.getLegend(barData, colors)}
									<BarChart
									  data={barData}
									  width={barWidth}
							          height={400}
								      colorScale={colorScale}
							          margin={{top: 30, bottom: 50, left: 50, right: 10}}
									  tooltipHtml={this.tooltip}
								      tooltipMode={"element"}
									/>
								</div>
							);
				}
			}

			return (
					<div>
						<h4>Analysis runs per service</h4>
			           	{progressIndicator}
						{barChart}
					</div>);
			}
		})
}

module.exports = ODFStats;
