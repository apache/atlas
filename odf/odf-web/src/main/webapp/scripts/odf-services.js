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
//js imports
var $ = require("jquery");
var bootstrap = require("bootstrap");

var React = require("react");
var ReactDOM = require("react-dom");
var LinkedStateMixin = require("react-addons-linked-state-mixin");
var ReactBootstrap = require("react-bootstrap");

var ODFGlobals = require("./odf-globals.js");
var AJAXCleanupMixin = require("./odf-mixins.js");
var configurationStore = require("./odf-utils.js").ConfigurationStore;
var servicesStore = require("./odf-utils.js").ServicesStore;

var Button = ReactBootstrap.Button;
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
var Image = ReactBootstrap.Image;

var DiscoveryServiceInfo = React.createClass({
	mixins : [AJAXCleanupMixin],

    testService() {
        const url = ODFGlobals.servicesUrl + "/" + this.props.dsreg.id;
        var req = $.ajax({
            url: url,
            contentType: "application/json",
            dataType: 'json',
            type: 'GET',
            success: function(data) {
                var type = "success";
                if (data.status != "OK") {
                    type = "danger";
                }
                var msg = "Status of ODF service '" + this.props.dsreg.name + "' is "+ data.status +" (" + data.message + ")";
                this.props.alertCallback({type: type, message: msg});
            }.bind(this),
            error: function(xhr, status, err) {
            	if(status != "abort" ){
            		console.error(url, status, err.toString());
            	}
         	    if(this.isMounted()){
         	    	var msg = "Service test failed: " + status + ", " + err.toString();
         	    	this.props.alertCallback({type: "danger", message: msg});
         	    }
            }.bind(this)
        });

        this.storeAbort(req.abort);
    },

    deleteService() {
        const url = ODFGlobals.servicesUrl + "/" + this.props.dsreg.id;
        $.ajax({
            url: url,
            type: 'DELETE',
            success: function(data) {
            	if(this.isMounted()){
            		this.props.refreshCallback();
            	}
            }.bind(this),
            error: function(xhr, status, err) {
            	if(status != "abort" ){
            		console.error(url, status, err.toString());
            	}
            	if(this.isMounted()){
	              var msg = "Service could not be deleted: " + status + ", " + err.toString();
	              this.props.alertCallback({type: "danger", message: msg});
			  }
			}.bind(this)
        });
    },

	render() {
    	var icon = "";
    	var imgUrl = this.props.dsreg.iconUrl;
    	//urls will be used directly.
    	if(imgUrl != null && (imgUrl.trim().startsWith("http://") || imgUrl.trim().startsWith("https://"))){
    		icon = imgUrl;
    	}else{
    		icon = ODFGlobals.servicesUrl + "/" + encodeURIComponent(this.props.dsreg.id) + "/image";
    	}

    	var endpointInfo = <span>No additional information</span>;
    	if (this.props.dsreg.endpoint.type == "Java") {
            endpointInfo = <span><em>Java class name</em>: {this.props.dsreg.endpoint.className}</span>;
    	}
		return (
				<Grid>
				  <Row className="show-grid">
				    <Col sm={1}>
		             <div >
		               <Image src={icon} rounded/>
	   	             </div>
		            </Col>
		            <Col sm={4}>
	             	  <b>{this.props.dsreg.name}</b>
	             	  <br/>
	             	  {this.props.dsreg.description}
	             	  <br/>
	             	  <a href={this.props.dsreg.link} target="_blank">More</a>
		              </Col>
	             	<Col sm={5}>
	             	  <em>Type</em>: {this.props.dsreg.endpoint.type}
	       	          <br/>
	       	          {endpointInfo}
	       	          <br/>
	       	          <em>ID</em>: {this.props.dsreg.id}
	       	          <br/>
	       	          <em>Protocol</em>: {this.props.dsreg.protocol}
	             	</Col>
	             	<Col sm={2}>
	             	  <Button bsStyle="primary" onClick={this.testService}>Test</Button>
	             	  <br/>
	             	 <Button bsStyle="warning" onClick={this.deleteService}>Delete</Button>
	             	</Col>
		         </Row>
		      </Grid>
		);
	}
});

var AddDiscoveryServiceButton = React.createClass({
  mixins: [LinkedStateMixin, AJAXCleanupMixin],

  getInitialState() {
	  return({showModal: false, serviceEndpointType: "Spark", parallelismCount: 2, serviceInterfaceType: "DataFrame"});
  },

  open() {
    this.setState({showModal: true, errorMessage: null});
  },

  close() {
    this.setState({showModal: false});
  },

  addService() {

	var newService = JSON.parse(JSON.stringify(this.state));
	delete newService.showModal;
	delete newService.errorMessage

	 var sparkEndpoint = {
        jar: newService.serviceApplication,
        className: newService.serviceClassName,
        inputMethod: newService.serviceInterfaceType,
        runtimeName: newService.serviceEndpointType
	 };

	newService.endpoint = sparkEndpoint;

    delete newService.serviceEndpointType;
    delete newService.serviceType;
    delete newService.serviceApplication;
    delete newService.serviceClassName;
    delete newService.serviceInterfaceType;

    $.ajax({
      url: ODFGlobals.servicesUrl,
      contentType: "application/json",
      type: 'POST',
      data: JSON.stringify(newService),
      success: function(data) {
		  if(this.isMounted()){
			  this.close();
			  this.props.refreshCallback();
		  }
      }.bind(this),
      error: function(xhr, status, err) {
		  if(this.isMounted()){
			var errorMsg = status;
			if(xhr.responseJSON && xhr.responseJSON.error){
    				errorMsg = xhr.responseJSON.error;
    		  	}
		    	var msg = "Service could not be added: " + errorMsg + ", " + err.toString();
	    	  	this.setState({errorMessage: msg});
		  }
      }.bind(this)
    });
  },

  render() {
    var alert = null;
    if (this.state.errorMessage) {
       alert = <Alert bsStyle="danger">{this.state.errorMessage}</Alert>;
    }

  	var endpointInput = null;
	endpointInput = <div>
						<Input type="text" valueLink={this.linkState("serviceApplication")} label="Application jar (or zip) file"/>
						<Input type="text" valueLink={this.linkState("serviceClassName")} label="Class name"/>
			            <Input type="select" valueLink={this.linkState("serviceInterfaceType")} label="Service interface type" placeholder="DataFrame">
			                <option value="DataFrame">DataFrame</option>
			                <option value="Generic">Generic</option>
		                </Input>
	            	</div>;

	  return(
				<span>
				<Button bsStyle="primary" bsSize="large" onClick={this.open}>Add ODF Service</Button>
				  <Modal show={this.state.showModal} onHide={this.close}>
						<Modal.Header closeButton>
						 	<Modal.Title>Add ODF Service</Modal.Title>
						</Modal.Header>
						<Modal.Body>
						{alert}
						  <Input type="text" ref="serviceName"  valueLink={this.linkState("name")} label="Name"/>
							<Input type="text" valueLink={this.linkState("description")} label="Description"/>
							<Input type="text" valueLink={this.linkState("id")} label="ID"/>
							<Input type="number" valueLink={this.linkState("parallelismCount")} label="Allowed parallel requests"/>
							<Input type="select" valueLink={this.linkState("serviceEndpointType")} label="Type" placeholder="Spark">
			                	<option value="Spark">Spark</option>
			                </Input>
							{endpointInput}
							<Input type="text" valueLink={this.linkState("iconUrl")} label="Icon (Optional)"/>
							<Input type="text" valueLink={this.linkState("link")} label="Link (Optional)"/>
					  </Modal.Body>
				    <Modal.Footer>
				    <Button bsStyle="primary" onClick={this.addService}>Add</Button>
				    <Button onClick={this.close}>Cancel</Button>
				    </Modal.Footer>
			     </Modal>
				</span>
			);
		}
});
module.exports = {DiscoveryServiceInfo: DiscoveryServiceInfo, AddDiscoveryServiceButton: AddDiscoveryServiceButton};
