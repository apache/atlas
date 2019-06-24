<!doctype html>
<!--
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
-->
<% response.setHeader("X-Frame-Options", "DENY"); %>
<!--[if lt IE 7]>      <html class="no-js lt-ie9 lt-ie8 lt-ie7"> <![endif]-->
<!--[if IE 7]>         <html class="no-js lt-ie9 lt-ie8"> <![endif]-->
<!--[if IE 8]>         <html class="no-js lt-ie9"> <![endif]-->
<!--[if lt IE 9]> 
<script type="text/javascript">
function Redirect() {
               window.location.assign("ieerror.html");
            }
Redirect();
</script>
<![endif]-->
<!--[if gt IE 7]>
    <script src="js/external_lib/es5-shim.min.js"></script>
    <script src="js/external_lib/respond.min.js"></script>
<![endif]-->
<!--[if gt IE 8]><!-->
<html class="no-js">
<!--<![endif]-->
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1">
    <title>Atlas Login</title>
    <meta name="description" content="">
    <meta name="viewport" content="width=device-width">
    <link href="js/libs/bootstrap/css/bootstrap.min.css" media="all" rel="stylesheet" type="text/css" id="bootstrap-css">
    <link href="js/libs/font-awesome/css/font-awesome.min.css" rel="stylesheet">
    <link href="css/login.css" media="all" rel="stylesheet" type="text/css" >
    <script src="js/libs/jquery/js/jquery.min.js" ></script>
    <script src="js/libs/jquery-placeholder/js/jquery.placeholder.js"></script>
    <script src="js/modules/atlasLogin.js" ></script>
  </head>
  <body class="login" style="">

<div id="wrapper">
  <div class="container-fluid">
        <div class="row">
            <div class="col-sm-4 col-sm-offset-4">
            <form  action="" method="post" accept-charset="utf-8">
                <div class="login-pane">
                    <h2 align="center">Apache <strong>Atlas</strong></h2>
                    <div class="input-group">
                        <span class="input-group-addon"><i class="fa fa-user"></i></span>
                        <input type="text" class="form-control" id="username" name="username" placeholder="Username" tabindex="1" required="" autofocus>
                    </div>
                    <div class="input-group">
                        <span class="input-group-addon"><i class="fa fa-lock"></i></span>
                        <input type="password" class="form-control" name="password" placeholder="Password" id="password" tabindex="2" autocomplete="off" required>    
                    </div>
                              <span id="errorBox" class="col-md-12 help-inline" style="color:#FF1A40;display:none;text-align:center;padding-bottom: 10px;"><span class="errorMsg"></span></span>
                    <input class="btn-atlas btn-block" name="submit" id="signIn" type="submit" value="Login">
                </div>
                </form>
            </div>
        </div>
  </div>
</div>
</body>
</html>