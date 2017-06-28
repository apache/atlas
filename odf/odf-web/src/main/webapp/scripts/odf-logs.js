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
var Input = ReactBootstrap.Input;

var REFRESH_DELAY = 5000;

var ODFLogViewer = React.createClass({
	mixins : [AJAXCleanupMixin],

	getInitialState : function(){
		return {logLevel : "ALL", log : ""};
	},

	getLogs : function() {
		const url = ODFGlobals.engineUrl + "/log?numberOfLogs=50&logLevel=" + this.state.logLevel;
        var req = $.ajax({
            url: url,
            contentType: "text/plain",
            type: 'GET',
            success: function(data) {
               this.setState({log: data});
            }.bind(this),
            error: function(xhr, status, err) {
              var msg = "ODF log request failed, " + err.toString();
              this.props.alertCallback({type: "danger", message: msg});
            }.bind(this)
        });

        this.storeAbort(req.abort);
	},

	componentWillMount : function() {
		this.getLogs();
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
			this.refreshInterval = window.setInterval(this.getLogs, REFRESH_DELAY);
		}
	},
	render : function(){
		return (<div>
					<h4>ODF system logs</h4>
					<h5>(This only works for the node this web application is running on, logs from other ODF nodes in a clustered environment will not be displayed)</h5>
					<Input label="Log level:" type="select" onChange={(el) => {this.setState({logLevel : el.target.value}); this.getLogs()}} value={this.state.logLevel}>
					<option value="ALL">ALL</option>
					<option value="FINE">FINE</option>
					<option value="INFO">INFO</option>
					<option value="WARNING">WARNING</option>
				</Input>
				<textarea disabled style={{width: '100%', height: '700px'}} value={this.state.log} /></div>);
	}
});

module.exports = ODFLogViewer;
