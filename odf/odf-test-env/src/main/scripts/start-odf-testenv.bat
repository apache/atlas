REM
REM Licensed under the Apache License, Version 2.0 (the "License");
REM you may not use this file except in compliance with the License.
REM You may obtain a copy of the License at
REM
REM   http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

setlocal

set JAVAEXE=%JAVA_HOME%\bin\java.exe
set PYTHON27EXE=python


REM you should not have to change anything below this line ;-)

set TESTENVDIR=%~dp0
set JETTY_HOME=%TESTENVDIR%jetty-distribution-9.2.10.v20150310
set KAFKA_PACKAGE_DIR=%TESTENVDIR%kafka_2.11-0.10.0.0
set ATLAS_HOME=%TESTENVDIR%apache-atlas-0.7-incubating-release

echo Delete logs
del /F /S /Q "C:\tmp\odftestenv-kafka-logs"
del /F /S /Q "C:\tmp\odftestenv-zookeeper"

echo Copy required files
xcopy %ATLAS_HOME%\conf\atlas-application.properties_windows %ATLAS_HOME%\conf\atlas-application.properties /Y

REM Workaround for issue #94 (Location of keystore files is hardcoded in Atlas config)
if not exist "C:\tmp\apache-atlas-0.7-incubating-release\conf" (mkdir "C:\tmp\apache-atlas-0.7-incubating-release\conf")
xcopy %ATLAS_HOME%\conf\keystore_ibmjdk.jceks C:\tmp\apache-atlas-0.7-incubating-release\conf /Y
xcopy %ATLAS_HOME%\conf\keystore_ibmjdk.jks C:\tmp\apache-atlas-0.7-incubating-release\conf /Y	

echo Start zookeeper:
start "Zookeeper" %KAFKA_PACKAGE_DIR%\bin\windows\zookeeper-server-start.bat %KAFKA_PACKAGE_DIR%\config\zookeeper.properties

timeout 5 /NOBREAK

echo Start kafka:
start "Kafka" %KAFKA_PACKAGE_DIR%\bin\windows\kafka-server-start.bat %KAFKA_PACKAGE_DIR%\config\server.properties

timeout 5 /NOBREAK

echo Start Atlas
start "Stop Atlas" %PYTHON27EXE% %ATLAS_HOME%\bin\atlas_stop.py
start "Start Atlas" %PYTHON27EXE% %ATLAS_HOME%\bin\atlas_start.py -port 21443

echo Start jetty
set JETTY_BASE=%TESTENVDIR%odfjettybase
rem set JETTY_BASE=%TESTENVDIR%base2
cd %JETTY_BASE%
start "Jetty" %JAVAEXE% -Dodf.zookeeper.connect=localhost:52181 -Datlas.url=https://localhost:21443 -Datlas.user=admin -Datlas.password=UR0+HOiApXG9B8SNpKN5ww== -Dodf.logspec=ALL,/tmp/odf-test-env-trace.log -jar %JETTY_HOME%\start.jar
