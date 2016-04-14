<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<%@ page import="org.apache.atlas.ApplicationProperties,org.apache.commons.configuration.Configuration" %>
<!DOCTYPE html>
<html lang="en">
	<head>
		<meta charset="utf-8">
		<title>Atlas Login</title>
		<meta name="description" content="description">
		<meta name="keyword" content="keywords">
		<meta name="viewport" content="width=device-width, initial-scale=1">
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css">
		<link href="http://netdna.bootstrapcdn.com/font-awesome/4.2.0/css/font-awesome.css" rel="stylesheet">
		<link href='http://fonts.googleapis.com/css?family=Righteous' rel='stylesheet' type='text/css'>
		<link href="/css/login.css" rel="stylesheet">
		<!-- HTML5 shim and Respond.js IE8 support of HTML5 elements and media queries -->
		<!--[if lt IE 9]>
				<script src="http://getbootstrap.com/docs-assets/js/html5shiv.js"></script>
				<script src="http://getbootstrap.com/docs-assets/js/respond.min.js"></script>
		<![endif]-->
		
		<script   src="https://code.jquery.com/jquery-2.2.1.min.js"   integrity="sha256-gvQgAFzTH6trSrAWoH1iPo9Xc96QxSZ3feW6kem+O00="   crossorigin="anonymous"></script>
		
	</head>
<body>
<div class="errorBox">
	<a href="javascript:void(0)" class="close" title="close"><i class="fa fa-times"></i></a>
	<div class="alert alert-danger">
	  	<strong>Error!</strong> Invalid User credentials.<br> Please try again.
	</div>
</div>

<div id="wrapper">
	<div class="container-fluid">
        <div class="row">
            <div class="col-sm-4 col-sm-offset-4">
            <form name='f' action='/j_spring_security_check' method='POST'>
                <div class="login-pane">
                    <h2 align="center">Apache <strong>Atlas</strong></h2>
                    <div class="input-group">
                        <span class="input-group-addon"><i class="glyphicon glyphicon-user"></i></span>
                        <input type='text' class="form-control" name='j_username' placeholder="Username" required >
                    </div>
                    <div class="input-group">
                        <span class="input-group-addon"><i class="glyphicon glyphicon-lock"></i></span>
                        <input type='password' name='j_password' class="form-control" placeholder="Password" required >
                        
                    </div>
                    <input class="btn-atlas btn-block" name="submit" type="submit" value="Login"/>
                    
                </div>
                </form>
            </div>
        </div>
    </div>
</div>

<script type="text/javascript">
$('body').ready(function(){
        var query = window.location.search.substring(1);
        var statusArr = query.split('=');
        var status = -1;
        if(statusArr.length > 0){
                status = statusArr[1];
        }
        if(status=="true"){
        	$('.errorBox').show();
        }
});
$('.close').click(function(){
	$('.errorBox').hide();
})
</script>

</body>
</html>
