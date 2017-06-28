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
//css imports
require("bootstrap/dist/css/bootstrap.min.css");
require("bootstrap-material-design/dist/css/bootstrap-material-design.min.css");
require("bootstrap-material-design/dist/css/ripples.min.css");
require("roboto-font/css/fonts.css");


//js imports
var $ = require("jquery");
var bootstrap = require("bootstrap");

var React = require("react");
var ReactDOM = require("react-dom");
var LinkedStateMixin = require("react-addons-linked-state-mixin");
var ReactBootstrap = require("react-bootstrap");

var ODFGlobals = require("./odf-globals.js");
var ODFStats = require("./odf-statistics.js");
var ODFSettings = require("./odf-settings.js");
var ODFServices = require("./odf-services.js");
var ODFBrowser = require("./odf-metadata-browser.js").ODFMetadataBrowser;
var ODFRequestBrowser = require("./odf-request-browser.js");
var AJAXCleanupMixin = require("./odf-mixins.js");
var configurationStore = require("./odf-utils.js").ConfigurationStore;
var servicesStore = require("./odf-utils.js").ServicesStore;
var AtlasHelper = require("./odf-utils.js").AtlasHelper;
var AnnotationStoreHelper = require("./odf-utils.js").AnnotationStoreHelper;
var OdfAnalysisRequest = require("./odf-analysis-request.js");
var LogViewer = require("./odf-logs.js");
//var Notifications = require("./odf-notifications.js");
var NewAnalysisRequestButton = OdfAnalysisRequest.NewAnalysisRequestButton;
var NewAnalysisRequestDialog = OdfAnalysisRequest.NewAnalysisRequestDialog;
var NewCreateAnnotationsButton = OdfAnalysisRequest.NewCreateAnnotationsButton;
var NewCreateAnnotationsDialog = OdfAnalysisRequest.NewCreateAnnotationsDialog;

var Button = ReactBootstrap.Button;
var Nav = ReactBootstrap.Nav;
var NavItem = ReactBootstrap.NavItem;
var Navbar = ReactBootstrap.Navbar;
var NavDropdown = ReactBootstrap.NavDropdown;
var MenuItem = ReactBootstrap.MenuItem;
var Jumbotron = ReactBootstrap.Jumbotron;
var Grid = ReactBootstrap.Grid;
var Row = ReactBootstrap.Row;
var Col = ReactBootstrap.Col;
var Table = ReactBootstrap.Table;
var Modal = ReactBootstrap.Modal;
var Input = ReactBootstrap.Input;
var Alert = ReactBootstrap.Alert;
var Panel = ReactBootstrap.Panel;
var Label = ReactBootstrap.Label;
var Input = ReactBootstrap.Input;
var ProgressBar = ReactBootstrap.ProgressBar;
var Image = ReactBootstrap.Image;
var ListGroup = ReactBootstrap.ListGroup;
var ListGroupItem = ReactBootstrap.ListGroupItem;
var Tabs = ReactBootstrap.Tabs;
var Tab = ReactBootstrap.Tab;
var Glyphicon = ReactBootstrap.Glyphicon;

var PerServiceStatusGraph = ODFStats.PerServiceStatusGraph;
var TotalAnalysisGraph = ODFStats.TotalAnalysisGraph;
var SystemDiagnostics = ODFStats.SystemDiagnostics;

////////////////////////////////////////////////////////////////
// toplevel navigation bar

const constants_ODFNavBar = {
  gettingStarted: "navKeyGettingStarted",
  configuration: "navKeyConfiguration",
  monitor: "navKeyMonitor",
  discoveryServices: "navKeyDiscoveryServices",
  data: "navKeyData",
  analysis: "navKeyAnalysis"
}

var ODFNavBar = React.createClass({
   render: function() {
       return (
         <Navbar inverse>
           <Navbar.Header>
             <Navbar.Brand>
               <b>Open Discovery Framework</b>
             </Navbar.Brand>
             <Navbar.Toggle />
           </Navbar.Header>
           <Navbar.Collapse>
             <Nav pullRight activeKey={this.props.activeKey} onSelect={this.props.selectCallback}>
               <NavItem eventKey={constants_ODFNavBar.gettingStarted} href="#">Getting Started</NavItem>
               <NavItem eventKey={constants_ODFNavBar.monitor} href="#">System Monitor</NavItem>
               <NavItem eventKey={constants_ODFNavBar.configuration} href="#">Settings</NavItem>
               <NavItem eventKey={constants_ODFNavBar.discoveryServices} href="#">Services</NavItem>
               <NavItem eventKey={constants_ODFNavBar.data} href="#">Data sets</NavItem>
               <NavItem eventKey={constants_ODFNavBar.analysis} href="#">Analysis</NavItem>
             </Nav>
           </Navbar.Collapse>
         </Navbar>
       );
   }
});



/////////////////////////////////////////////////////////////////////////////////////////
// Configuration page

var ConfigurationPage = React.createClass({
  componentWillMount() {
      this.props.alertCallback({type: ""});
  },

  render: function() {
    return (
    <div className="jumbotron">
      <Tabs position="left" defaultActiveyKey={1}>
        <Tab eventKey={1} title="General">
          <ODFSettings.ODFConfigPage alertCallback={this.props.alertCallback}/>
        </Tab>
        <Tab eventKey={2} title="Spark settings">
          <ODFSettings.SparkConfigPage alertCallback={this.props.alertCallback}/>
        </Tab>
        <Tab eventKey={3} title="User-defined">
          <ODFSettings.UserDefinedConfigPage alertCallback={this.props.alertCallback}/>
        </Tab>
      </Tabs>
      </div>
      );
  }

});

const GettingStartedPage = React.createClass({
  getInitialState() {
     return ({version: "NOTFOUND"});
  },

  componentWillMount() {
     this.props.alertCallback({type: ""});
     $.ajax({
         url: ODFGlobals.engineUrl + "/version",
         type: 'GET',
         success: function(data) {
             this.setState(data);
         }.bind(this)
       });
  },

  render: function() {
    var divStyle = {
      marginLeft: "80px",
      marginRight: "80px"
    };
    return (
      <Jumbotron>
      <div style={divStyle}>
         <h2>Welcome to the Open Discovery Framework Console</h2>
         <p/>The "Open Discovery Framework" (ODF) is an open metadata-based platform
         that strives to be a common home for different analytics technologies
         that discover characteristics of data sets and relationships between
         them (think "AppStore for discovery algorithms").
         Using ODF, applications can leverage new discovery algorithms and their
         results with minimal integration effort.
         <p/>
         This console lets you administer and configure your ODF system, as well as
         run analyses and browse their results.
         <p/>
         <p><Button target="_blank" href="doc" bsStyle="primary">Open Documentation</Button></p>
         <p><Button target="_blank" href="swagger" bsStyle="success">Show API Reference</Button></p>
         <p/>
		 Version: {this.state.version}
         </div>
       </Jumbotron>

      )
  }

});

/////////////////////////////////////////////////////////////////////
// monitor page
var StatusGraphs = React.createClass({

	selectTab : function(key){
		this.setState({key});
	},

	getInitialState() {
	    return {
	      key: "system_state"
	    };
	 },

	render : function() {
		var divStyle = {
		     marginLeft: "20px"
	    };

		return (
			<div>
				<Tabs position="left" activeKey={this.state.key} onSelect={this.selectTab}>
					<Tab eventKey={"system_state"} title="System state">
						<div style={divStyle}>
							<TotalAnalysisGraph visible={this.state.key == "system_state"} alertCallback={this.props.alertCallback}/>
							<PerServiceStatusGraph visible={this.state.key == "system_state"} alertCallback={this.props.alertCallback}/>
						</div>
					</Tab>
				    <Tab eventKey={"diagnostics"} title="Diagnostics">
						<div style={divStyle}>
							<SystemDiagnostics visible={this.state.key == "diagnostics"} alertCallback={this.props.alertCallback}/>
						</div>
					</Tab>
					<Tab eventKey={"logs"} title="System logs">
						<div style={divStyle}>
							<LogViewer visible={this.state.key == "logs"} alertCallback={this.props.alertCallback}/>
						</div>
					</Tab>
				</Tabs>
			</div>
         );
	}


});

var MonitorPage = React.createClass({
	mixins : [AJAXCleanupMixin],

	getInitialState() {
		return ( {
				monitorStatusVisible: false,
				monitorStatusStyle:"success",
				monitorStatusMessage: "OK",
				monitorWorkInProgress: false
		});
	},

	componentWillMount() {
	   this.props.alertCallback({type: ""});
	},

	checkHealth() {
		this.setState({monitorWorkInProgress: true, monitorStatusVisible: false});
	    var url = ODFGlobals.engineUrl + "/health";
		var req = $.ajax({
	         url: url,
	         dataType: 'json',
	         type: 'GET',
	         success: function(data) {
	        	 var status = data.status;
	        	 var newState = {
	        		monitorStatusVisible: true,
	        		monitorWorkInProgress: false
	        	 };

	        	 if (status == "OK") {
	        		 newState.monitorStatusStyle = "success";
	        	 } else if (status == "WARNING") {
	        		 newState.monitorStatusStyle = "warning";
	        	 } else if (status == "ERROR") {
	        		 newState.monitorStatusStyle = "danger";
	        	 }
	        	 // TODO show more than just the first message
        		 newState.monitorStatusMessage = "Status: " + status + ". " + data.messages[0];

	        	 this.setState(newState);
	         }.bind(this),
	         error: function(xhr, status, err) {
	      	   if(this.isMounted()){
	      		   this.setState({
	        	   monitorStatusVisible: true,
	        	   monitorStatusStyle:"danger",
	        	   monitorStatusMessage: "An error occured: " + err.toString(),
	        	   monitorWorkInProgress: false});
	      	   };
	         }.bind(this)
	        });
		this.storeAbort(req.abort);
	},

	performRestart : function(){
		$.ajax({
		      url: ODFGlobals.engineUrl + "/shutdown",
		      contentType: "application/json",
		      type: 'POST',
		      data: JSON.stringify({restart: "true"}),
		      success: function(data) {
		  			this.setState({monitorStatusVisible : true, monitorStatusStyle: "info", monitorStatusMessage: "Restart in progress..."});
		      }.bind(this),
		      error: function(xhr, status, err) {
		  			this.setState({monitorStatusVisible : true, monitorStatusStyle: "warning", monitorStatusMessage: "Restart request failed"});
		      }.bind(this)
		    });
	},

	render() {
	  var divStyle = {
		      marginLeft: "20px"
		    };
	  var monitorStatus = null;
	  if (this.state.monitorStatusVisible) {
		  monitorStatus = <Alert bsStyle={this.state.monitorStatusStyle}>{this.state.monitorStatusMessage}</Alert>;
	  }
	  var progressIndicator = null;
	  if (this.state.monitorWorkInProgress) {
		  progressIndicator = <Image src="img/lg_proc.gif" rounded />;
	  }
	  return (
	    	<div className="jumbotron">
	    	<h3>System health</h3>
	    	  <div style={divStyle}>
	           	<Button className="btn-raised" bsStyle="primary" disabled={this.state.monitorWorkInProgress} onClick={this.checkHealth}>Check health</Button>
	           	<Button className="btn-raised" bsStyle="warning" onClick={this.performRestart}>Restart ODF</Button>
	           	{progressIndicator}
	           	{monitorStatus}
	           	<hr/>
	           	<div>
	           	</div>
	           	<StatusGraphs alertCallback={this.props.alertCallback}/>
	    	  </div>
	    	</div>
	  );
	}

});

//////////////////////////////////////////////////////
// discovery services page
var DiscoveryServicesPage = React.createClass({
  mixins : [AJAXCleanupMixin],

  getInitialState() {
	  return ({discoveryServices: []});
  },

  loadDiscoveryServices() {
	  // clear alert
    this.props.alertCallback({type: "", message: ""});

	var req = $.ajax({
	    url: ODFGlobals.servicesUrl,
	    dataType: 'json',
	    type: 'GET',
	    success: function(data) {
	       this.setState({discoveryServices: data});
	    }.bind(this),
	    error: function(xhr, status, err) {
    	   if(this.isMounted()){
    		   var msg = "Error while reading ODF services: " + err.toString();
    		   this.props.alertCallback({type: "danger", message: msg});
    	   }
	    }.bind(this)
	  });

	this.storeAbort(req.abort);
  },

  componentDidMount() {
	  this.loadDiscoveryServices();
  },

  render: function() {
	var services = $.map(
        this.state.discoveryServices,
        function(dsreg) {
          return <tr key={dsreg.id}>
                  <td>
                     <ODFServices.DiscoveryServiceInfo dsreg={dsreg} refreshCallback={this.loadDiscoveryServices} alertCallback={this.props.alertCallback}/>
                  </td>
                 </tr>
        }.bind(this)
    );

	return (
	     <div className="jumbotron">
           <h3>Services</h3>
           This page lets you manage the services for this ODF instance.
           You can add services manually by clicking the <em>Add Service</em> button or
           register remote services (e.g. deployed on Bluemix) you have built with the ODF service developer kit by
           clicking the <em>Register remote services</em> link.
           <p/>
					 <ODFServices.AddDiscoveryServiceButton refreshCallback={this.loadDiscoveryServices}/>
           <p/>
	       	<Table bordered responsive>
	         <tbody>
	         {services}
             </tbody>
          </Table>
	     </div>
	);
  }

});

//////////////////////////////////////////////////////////////
// Analysis Page
var AnalysisRequestsPage = React.createClass({
  mixins : [AJAXCleanupMixin],

  getInitialState() {
      return {recentAnalysisRequests: null, config: {}, services : []};
  },

  componentWillReceiveProps : function(nextProps){
  	var selection = null;
	if(nextProps.navAddition && nextProps.navAddition.length > 0 && nextProps.navAddition[0] && nextProps.navAddition[0].length > 0){
		var jsonAddition = {};

		try{
			jsonAddition = JSON.parse(decodeURIComponent(nextProps.navAddition[0]));
		}catch(e){

		}

		if(jsonAddition.requestId){
			$.each(this.state.recentAnalysisRequests, function(key, tracker){
				var reqId = jsonAddition.requestId;

				if(tracker.request.id == reqId){
					selection = reqId;
				}
			}.bind(this));
		}else if(jsonAddition.id && jsonAddition.repositoryId){
			selection = jsonAddition;
		}
	}

	if(selection != this.state.selection){
		this.setState({selection : selection});
	}
  },

  componentDidMount() {
	  if(!this.refreshInterval){
		  this.refreshInterval = window.setInterval(this.refreshAnalysisRequests, 5000);
	  }
      this.initialLoadServices();
      this.initialLoadRecentAnalysisRequests();
  },

  componentWillUnmount : function() {
	  if(this.refreshInterval){
		  window.clearInterval(this.refreshInterval);
	  }
  },

  getDiscoveryServiceNameFromId(id) {
      var servicesWithSameId = this.state.services.filter(
         function(dsreg) {
             return dsreg.id == id;
         }
      );
      if (servicesWithSameId.length > 0) {
        return servicesWithSameId[0].name;
      }
      return null;
  },

  refreshAnalysisRequests : function(){
	  var req = configurationStore.readConfig(
		      function(config) {
		          this.setState({config: config});
		          const url = ODFGlobals.analysisUrl + "?offset=0&limit=20";
		          $.ajax({
		            url: url,
		            dataType: 'json',
		            type: 'GET',
		            success: function(data) {
		            	$.each(data.analysisRequestTrackers, function(key, tracker){
		                	//collect service names by id and add to json so that it can be displayed later
		            		$.each(tracker.discoveryServiceRequests, function(key, request){
			            		var serviceName = this.getDiscoveryServiceNameFromId(request.discoveryServiceId);
			            		request.discoveryServiceName = serviceName;
		            		}.bind(this));
		            	}.bind(this));
		                this.setState({recentAnalysisRequests: data.analysisRequestTrackers});
		            }.bind(this),
		            error: function(xhr, status, err) {
		            	if(status != "abort" ){
		            		console.error(url, status, err.toString());
		            	}
		            	if(this.isMounted()){
		            	  var msg = "Error while refreshing recent analysis requests: " + err.toString();
		            	  this.props.alertCallback({type: "danger", message: msg});
		            	}
		            }.bind(this)
		          });
		      }.bind(this),
	      this.props.alertCallback
	    );

	    this.storeAbort(req.abort);
  },

  initialLoadServices() {
	this.setState({services: null});

    var req = servicesStore.getServices(
      function(services) {
          this.setState({services: services});
      }.bind(this),
      this.props.alertCallback
    );

    this.storeAbort(req.abort);
  },

  initialLoadRecentAnalysisRequests() {
	this.setState({recentAnalysisRequests: null});

    var req = configurationStore.readConfig(
      function(config) {
          this.setState({config: config});
          const url = ODFGlobals.analysisUrl + "?offset=0&limit=20";
          $.ajax({
            url: url,
            dataType: 'json',
            type: 'GET',
            success: function(data) {
            	var selection = null;
            	$.each(data.analysisRequestTrackers, function(key, tracker){
            		if(this.props.navAddition && this.props.navAddition.length > 0 && this.props.navAddition[0].length > 0){
            			var reqId = "";
            			try{
            				reqId = JSON.parse(decodeURIComponent(this.props.navAddition[0])).requestId
            			}catch(e){

            			}
            			if(tracker.request.id == reqId){
            				selection = reqId;
            			}
        			}

                	//collect service names by id and add to json so that it can be displayed later
            		$.each(tracker.discoveryServiceRequests, function(key, request){
	            		var serviceName = this.getDiscoveryServiceNameFromId(request.discoveryServiceId);
	            		request.discoveryServiceName = serviceName;
            		}.bind(this));
            	}.bind(this));

            	var newState = {recentAnalysisRequests: data.analysisRequestTrackers};
            	if(selection){
            		newState.selection = selection;
            	}

               this.setState(newState);
            }.bind(this),
            error: function(xhr, status, err) {
            	if(status != "abort" ){
            		console.error(url, status, err.toString());
            	}
            	if(this.isMounted()){
            	  var msg = "Error while loading recent analysis requests: " + err.toString();
            	  this.props.alertCallback({type: "danger", message: msg});
            	}
            }.bind(this)
          });
      }.bind(this),
      this.props.alertCallback
    );

    this.storeAbort(req.abort);
  },

  cancelAnalysisRequest(tracker) {
      var url = ODFGlobals.analysisUrl + "/" + tracker.request.id + "/cancel";

      $.ajax({
          url: url,
          type: 'POST',
          success: function() {
			  if(this.isMounted()){
				  this.refreshAnalysisRequests();
			  }
          }.bind(this),
          error: function(xhr, status, err) {
        	  if(status != "abort" ){
          		console.error(url, status, err.toString());
        	  }

        	  var errMsg = null;
        	  if(err == "Forbidden"){
        		  errMsg = "only analyses that have not been started yet can be cancelled!";
        	  }else if(err == "Bad Request"){
        		  errMsg = "the requested analysis could not be found!";
        	  }
        	  if(this.isMounted()){
				  var msg = "Analysis could not be cancelled: " + (errMsg ? errMsg : err.toString());
				  if(this.props.alertCallback){
					  this.props.alertCallback({type: "danger", message: msg});
				  }
        	  }
          }.bind(this)
      });
  },

  viewResultAnnotations : function(target){
	  this.setState({
			resultAnnotations : null,
			showAnnotations: true
		});
	  var req = AnnotationStoreHelper.loadAnnotationsForRequest(target.request.id,
			function(data){
				this.setState({
					resultAnnotations : data.annotations
				});
			}.bind(this),
			function(error){
				console.error('Annotations could not be loaded ' + error);
			}
		);
	  this.storeAbort(req.abort);
  },

  viewInAtlas : function(target){
	  var repo =  target.request.dataSets[0].repositoryId;
	  repo = repo.split("atlas:")[1];
      var annotationQueryUrl = repo + "/#!/search?query=from%20ODFAnnotation%20where%20analysisRun%3D'"+ target.request.id + "'";
	  var win = window.open(annotationQueryUrl, '_blank');
  },

  render : function() {
    var loadingImg = null;
    if(this.state.recentAnalysisRequests == null){
    	loadingImg = <Image src="img/lg_proc.gif" rounded />;
    }
    var requestActions = [
                           {
                        	   assetType: ["requests"],
                        	   actions : [
                	              {
                	            	  label: "Cancel analysis",
                	            	  func: this.cancelAnalysisRequest,
                	            	  filter: function(obj){
                	            		  var val = obj.status;
                	            		  if (val == "INITIALIZED" || val == "IN_DISCOVERY_SERVICE_QUEUE") {
                	            			  return true;
                	            		  }
                	            		  return false;
                	            	  }
                	              },
                	              {
                	            	  label: "View results",
                	            	  func: this.viewResultAnnotations
                	              },
                	              {
                	            	  label: "View results in atlas",
                	            	  func: this.viewInAtlas
                	              }
                	           ]
                           	}
                           ];
    return (
    		<div className="jumbotron">
			   <h3>Analysis requests</h3>
			   <div>
		        Click Refresh to refresh the list of existing analysis requests.
		        Only the last 20 valid requests are shown.
		         <p/>
		        <NewAnalysisRequestButton bsStyle="primary" onClose={this.refreshAnalysisRequests} alertCallback={this.props.alertCallback}/>
		        <NewCreateAnnotationsButton bsStyle="primary" onClose={this.refreshAnalysisRequests} alertCallback={this.props.alertCallback}/>

		        <Button bsStyle="success" onClick={this.refreshAnalysisRequests}>Refresh</Button> &nbsp;
            	{loadingImg}
		        <ODFRequestBrowser registeredServices={this.state.config.registeredServices} actions={requestActions} ref="requestBrowser" selection={this.state.selection} assets={this.state.recentAnalysisRequests}/>
		       </div>
            	<Modal show={this.state.showAnnotations} onHide={function(){this.setState({showAnnotations : false})}.bind(this)}>
	            	<Modal.Header closeButton>
	                	<Modal.Title>Analysis results for analysis {this.state.resultTarget}</Modal.Title>
		             </Modal.Header>
		             <Modal.Body>
		             	<ODFBrowser ref="resultBrowser" type={"annotations"} assets={this.state.resultAnnotations} />
		             </Modal.Body>
		             <Modal.Footer>
		            <Button onClick={function(){this.setState({showAnnotations : false})}.bind(this)}>Close</Button>
		            </Modal.Footer>
            	</Modal>
		    </div>
    );
  }

});

var AnalysisDataSetsPage = React.createClass({
  mixins : [AJAXCleanupMixin],

  componentDidMount() {
      this.loadDataFiles();
      this.loadTables();
      this.loadDocuments();
  },

  getInitialState() {
      return ({	showDataFiles: true,
    	  		showHideDataFilesIcon: "chevron-up",
    	  		showTables: true,
    	  		showHideTablesIcon: "chevron-up",
    	  		showDocuments: true,
    	  		showHideDocumentsIcon: "chevron-up",
    	  		config: null});
  },

  componentWillReceiveProps : function(nextProps){
	if(nextProps.navAddition && nextProps.navAddition.length > 0 && nextProps.navAddition[0]){
		this.setState({selection : nextProps.navAddition[0]});
	}else{
		this.setState({selection : null});
	}
  },

  showHideDataFiles() {
	  this.setState({showDataFiles: !this.state.showDataFiles, showHideDataFilesIcon: (!this.state.showDataFiles? "chevron-up" : "chevron-down")});
  },

  showHideTables() {
	  this.setState({showTables: !this.state.showTables, showHideTablesIcon: (!this.state.showTables? "chevron-up" : "chevron-down")});
  },

  showHideDocuments() {
	  this.setState({showDocuments: !this.state.showDocuments, showHideDocumentsIcon: (!this.state.showDocuments ? "chevron-up" : "chevron-down")});
  },

  createAnnotations : function(target){
		this.setState({showCreateAnnotationsDialog: true, selectedAsset : target.reference.id});
  },

  startAnalysis : function(target){
		this.setState({showAnalysisRequestDialog: true, selectedAsset : target.reference.id});
  },

  viewInAtlas : function(target){
	  var win = window.open(target.reference.url, '_blank');
	  win.focus();
  },

  loadDataFiles : function(){
	  var  resultQuery = "from DataFile";
	  this.setState({
			dataFileAssets : null
	  });
	  var req = AtlasHelper.searchAtlasMetadata(resultQuery,
			function(data){
				this.setState({
					dataFileAssets : data
				});
			}.bind(this),
			function(error){

			}
		);
	  this.storeAbort(req.abort);
  },

  loadTables : function(){
	  var  resultQuery = "from Table";
	  this.setState({
			tableAssets : null
	  });
	  var req = AtlasHelper.searchAtlasMetadata(resultQuery,
			function(data){
				this.setState({
					tableAssets : data
				});
			}.bind(this),
			function(error){

			}
		);
	  this.storeAbort(req.abort);
  },

  loadDocuments : function(){
	  var  resultQuery = "from Document";
	  this.setState({
			docAssets : null
	  });
	  var req = AtlasHelper.searchAtlasMetadata(resultQuery,
			function(data){
				this.setState({
					docAssets : data
				});
			}.bind(this),
			function(error){

			}
		);
	  this.storeAbort(req.abort);
  },

  render() {
    var actions = [
             {
        	   assetType: ["DataFiles", "Tables", "Documents"],
        	   actions : [
	              {
	            	  label: "Start analysis (annotation types)",
	            	  func: this.createAnnotations
	              } ,
	              {
	            	  label: "Start analysis (service sequence)",
	            	  func: this.startAnalysis
	              } ,
	              {
	            	  label: "View in atlas",
	            	  func: this.viewInAtlas
	              }
        	    ]
	         }
	     ];

    return (
    		<div className="jumbotron">
    		   <h3>Data sets</h3>
		       <div>
		       	 <NewAnalysisRequestDialog alertCallback={this.props.alertCallback} dataSetId={this.state.selectedAsset} show={this.state.showAnalysisRequestDialog} onClose={function(){this.setState({showAnalysisRequestDialog: false});}.bind(this)} />
		       	 <NewCreateAnnotationsDialog alertCallback={this.props.alertCallback} dataSetId={this.state.selectedAsset} show={this.state.showCreateAnnotationsDialog} onClose={function(){this.setState({showCreateAnnotationsDialog: false});}.bind(this)} />
		         Here are all data sets of the metadata repository that are available for analysis.
		         <p/>
		         <Panel collapsible expanded={this.state.showDataFiles} header={
		        		 <div style={{textAlign:"right"}}>
				         	<span style={{float: "left"}}>Data Files</span>
				         	<Button bsStyle="primary" onClick={function(){this.loadDataFiles();}.bind(this)}>
				         		Refresh
				         	</Button>
			         		<Button onClick={this.showHideDataFiles}>
			         			<Glyphicon glyph={this.state.showHideDataFilesIcon} />
			         		</Button>
			         	</div>}>
		            	<ODFBrowser ref="dataFileBrowser" type={"DataFiles"} selection={this.state.selection} actions={actions} assets={this.state.dataFileAssets} />
		         </Panel>
		         <Panel collapsible expanded={this.state.showTables} header={
		        		 <div style={{textAlign:"right"}}>
				         	<span style={{float: "left"}}>Relational Tables</span>
				         	<Button bsStyle="primary" onClick={function(){this.loadTables();}.bind(this)}>
				         		Refresh
				         	</Button>
			         		<Button onClick={this.showHideTables}>
			         			<Glyphicon glyph={this.state.showHideTablesIcon} />
			         		</Button>
			         	</div>}>
		            	<ODFBrowser ref="tableBrowser" type={"Tables"} actions={actions} assets={this.state.tableAssets} />
		         </Panel>
		         <Panel collapsible expanded={this.state.showDocuments}  header={
		        		 <div style={{textAlign:"right"}}>
		        		 	<span style={{float: "left"}}>Documents</span>
		        		 	<Button bsStyle="primary" onClick={function(){this.loadDocuments();}.bind(this)}>
		        		 		Refresh
		        		 	</Button>
		        		 	<Button onClick={this.showHideDocuments}>
			         			<Glyphicon glyph={this.state.showHideDocumentsIcon} />
			         		</Button>
			         	</div>}>
		     			<ODFBrowser ref="docBrowser" type={"Documents"} actions={actions} assets={this.state.docAssets}/>
		         </Panel>
		       </div>
		    </div>
		     );
  }

});


////////////////////////////////////////////////////////////////////////
// main component
var ODFUI = React.createClass({

   componentDidMount: function() {
	   $(window).bind("hashchange", this.parseUrl);
	   this.parseUrl();
   },

   parseUrl : function(){
	  var target = constants_ODFNavBar.gettingStarted;
	  var navAddition = null;
	  var hash = document.location.hash;
	  if(hash && hash.length > 1){
		  hash = hash.split("#")[1];
		  var split = hash.split("/");
		  var navHash = split[0];
		  if(split.length > 0){
			  navAddition = split.slice(1);
		  }
		  if(constants_ODFNavBar[navHash]){
			  target = constants_ODFNavBar[navHash];
		  }
	  }
	  this.setState({
		  activeNavBarItem: target,
	      navAddition: navAddition}
	  );
  },

  getInitialState: function() {
	  return ({
	      activeNavBarItem: constants_ODFNavBar.gettingStarted,
	      navAddition: null,
	      globalAlert: {
	        type: "",
	        message: ""
	      }
	  });
  },

  handleNavBarSelection: function(selection) {
	  $.each(constants_ODFNavBar, function(key, ref){
		  if(ref == selection){
			  document.location.hash = key;
		  }
	  });
    this.setState({ activeNavBarItem: selection });
  },

  handleAlert: function(alertInfo) {
    this.setState({ globalAlert: alertInfo });
  },

  render: function() {
    var alertComp = null;
    if (this.state.globalAlert.type != "") {
       alertComp = <Alert bsStyle={this.state.globalAlert.type}>{this.state.globalAlert.message}</Alert>;
    }

    var contentComponent = <GettingStartedPage alertCallback={this.handleAlert}/>;
    if (this.state.activeNavBarItem == constants_ODFNavBar.configuration) {
       contentComponent = <ConfigurationPage alertCallback={this.handleAlert}/>;
    } else if (this.state.activeNavBarItem == constants_ODFNavBar.discoveryServices) {
       contentComponent = <DiscoveryServicesPage alertCallback={this.handleAlert}/>;
    } else if (this.state.activeNavBarItem == constants_ODFNavBar.monitor) {
       contentComponent = <MonitorPage alertCallback={this.handleAlert}/>;
    } else if (this.state.activeNavBarItem == constants_ODFNavBar.analysis) {
       contentComponent = <AnalysisRequestsPage navAddition={this.state.navAddition} alertCallback={this.handleAlert}/>;
    } else if (this.state.activeNavBarItem == constants_ODFNavBar.data) {
       contentComponent = <AnalysisDataSetsPage navAddition={this.state.navAddition} alertCallback={this.handleAlert}/>;
    }

    var divStyle = {
      marginLeft: "80px",
      marginRight: "80px"
    };

    return (
        <div>
           <ODFNavBar activeKey={this.state.activeNavBarItem} selectCallback={this.handleNavBarSelection}></ODFNavBar>
           <div style={divStyle}>
              {alertComp}
              {contentComponent}
           </div>
        </div>
    );
  }
});

var div = $("#odf-toplevel-div")[0];
ReactDOM.render(<ODFUI/>, div);
