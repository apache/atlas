#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import getpass
import glob
import os
import re
import platform
import subprocess
import sys
import time
import errno
import socket
from re import split
from time import sleep


BIN = "bin"
LIB = "lib"
CONF = "conf"
LOG = "logs"
WEBAPP = "server" + os.sep + "webapp"

DATA = "data"
REST_CONF = "REST_CONF"
REST_LOG = "REST_LOG_DIR"
REST_PID = "REST_PID_DIR"
REST_WEBAPP = "REST_EXPANDED_WEBAPP_DIR"
REST_SERVER_OPTS = "REST_SERVER_OPTS"
REST_OPTS = "REST_OPTS"
REST_SERVER_HEAP = "REST_SERVER_HEAP"
REST_DATA = "REST_DATA_DIR"
REST_HOME = "REST_HOME_DIR"
ATLAS_HOME = "ATLAS_HOME_DIR"
REST_WAR = "REST_WAR"

ENV_KEYS = ["JAVA_HOME", REST_OPTS, REST_SERVER_OPTS, REST_SERVER_HEAP, REST_LOG, REST_PID, REST_CONF,
            "RESTCPPATH", REST_DATA, REST_HOME, REST_WEBAPP, REST_WAR]

IS_WINDOWS = platform.system() == "Windows"
ON_POSIX = 'posix' in sys.builtin_module_names
CONF_FILE="atlas-application.properties"


TOPICS_TO_CREATE="atlas.notification.topics"
REST_HTTP_PORT="atlas.rest.server.http.port"
REST_HTTPS_PORT="atlas.rest.server.https.port"
DEFAULT_REST_HTTP_PORT="41000"
DEFAULT_REST_HTTPS_PORT="41443"
REST_ENABLE_TLS="atlas.rest.notification.enableTLS"
REST_SERVER_BIND_ADDRESS="atlas.rest.server.bind.address"
DEFAULT_REST_SERVER_HOST="localhost"

DEBUG=False

def scriptDir():
    """
    get the script path
    """
    return os.path.dirname(os.path.realpath(__file__))


def restDir():
    home = os.path.dirname(scriptDir())
    return os.environ.get(REST_HOME, home)

def atlasDir():
    home = os.path.dirname(scriptDir())
    return os.environ.get(ATLAS_HOME, home)

def libDir(dir) :
    return os.path.join(dir, LIB)

def confDir(dir):
    localconf = os.path.join(dir, CONF)
    return os.environ.get(REST_CONF, localconf)

def logDir(dir):
    localLog = os.path.join(dir, LOG)
    return os.environ.get(REST_LOG, localLog)

def pidFile(dir):
    localPid = os.path.join(dir, LOG)
    return os.path.join(os.environ.get(REST_PID, localPid), 'rest.pid')

def dataDir(dir):
    data = os.path.join(dir, DATA)
    return os.environ.get(REST_DATA, data)

def webAppDir(dir):
    webapp = os.path.join(dir, WEBAPP)
    return os.environ.get(REST_WEBAPP, webapp)


def resolveRestWarPath():
    war_dir = os.path.join(restDir(), "server", "webapp")
    env_war = os.environ.get(REST_WAR)
    if env_war and os.path.isfile(env_war):
        return env_war
    for war_name in ("rest-notification.war", "rest-notification-webapp.war"):
        candidate = os.path.join(war_dir, war_name)
        if os.path.isfile(candidate):
            return candidate
    if os.path.isdir(war_dir):
        matches = sorted(
            p for p in glob.glob(os.path.join(war_dir, "rest-notification*.war"))
            if os.path.isfile(p)
        )
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            for preferred in ("rest-notification.war", "rest-notification-webapp.war"):
                preferred_path = os.path.join(war_dir, preferred)
                if preferred_path in matches:
                    return preferred_path
            return matches[0]
    raise EnvironmentError(
        "REST notification WAR not found. Install under %s as rest-notification.war, "
        "rest-notification-webapp.war, or rest-notification*.war, or set %s to the WAR path."
        % (war_dir, REST_WAR)
    )


def expandWebApp(dir):
    webappDir = webAppDir(dir)
    webAppMetadataDir = os.path.join(webappDir, "rest-notification")
    d = os.sep
    if not os.path.exists(os.path.join(webAppMetadataDir, "WEB-INF")):
        try:
            os.makedirs(webAppMetadataDir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e
            pass
        restWarPath = resolveRestWarPath()
        if isCygwin():
            restWarPath = convertCygwinPath(restWarPath)
        os.chdir(webAppMetadataDir)
        jar(restWarPath)


def dirMustExist(dirname):
    if not os.path.exists(dirname):
        os.mkdir(dirname)
    return dirname

def executeEnvSh(confDir):
    envscript = '%s/rest-env.sh' % confDir
    if not IS_WINDOWS and os.path.exists(envscript):
        envCmd = 'source %s && env' % envscript
        command = ['bash', '-c', envCmd]

        proc = subprocess.Popen(command, stdout = subprocess.PIPE)

        for line in proc.stdout:
            (key, _, value) = line.decode('utf8').strip().partition("=")
            if key in ENV_KEYS:
                os.environ[key] = value

        proc.communicate()



def java(classname, args, classpath, jvm_opts_list, logdir=None):
    java_home = os.environ.get("JAVA_HOME", None)
    if java_home:
        prg = os.path.join(java_home, "bin", "java")
    else:
        prg = which("java")

    if prg is None:
        raise EnvironmentError('The java binary could not be found in your path or JAVA_HOME')

    commandline = [prg]
    commandline.extend(jvm_opts_list)
    commandline.append("-classpath")
    commandline.append(classpath)
    commandline.append(classname)
    commandline.extend(args)
    return runProcess(commandline, logdir)


def jar(path):
    java_home = os.environ.get("JAVA_HOME", None)
    if java_home:
        prg = os.path.join(java_home, "bin", "jar")
    else:
        prg = which("jar")

    if prg is None:
        raise EnvironmentError('The jar binary could not be found in your path or JAVA_HOME')

    commandline = [prg]
    commandline.append("-xf")
    commandline.append(path)
    process = runProcess(commandline)
    process.wait()

def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

def which(program):

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None



def runProcess(commandline, logdir=None, shell=False, wait=False):
    """
    Run a process
    :param commandline: command line
    :return:the return code
    """
    global finished
    debug ("Executing : %s" % str(commandline))
    timestr = time.strftime("rest.%Y%m%d-%H%M%S")
    stdoutFile = None
    stderrFile = None
    if logdir:
        stdoutFile = open(os.path.join(logdir, timestr + ".out"), "w")
        stderrFile = open(os.path.join(logdir,timestr + ".err"), "w")

    p = subprocess.Popen(commandline, stdout=stdoutFile, stderr=stderrFile, shell=shell)

    if wait:
        p.communicate()

    return p


def print_output(name, src, toStdErr):
    """
    Relay the output stream to stdout line by line
    :param name:
    :param src: source stream
    :param toStdErr: flag set if stderr is to be the dest
    :return:
    """

    global needPassword
    debug ("starting printer for %s" % name )
    line = ""
    while not finished:
        (line, done) = read(src, line)
        if done:
            out(toStdErr, line + "\n")
            flush(toStdErr)
            if line.find("Enter password for") >= 0:
                needPassword = True
            line = ""
    out(toStdErr, line)
    # closedown: read remainder of stream
    c = src.read(1)
    while c!="" :
        c = c.decode('utf-8')
        out(toStdErr, c)
        if c == "\n":
            flush(toStdErr)
        c = src.read(1)
    flush(toStdErr)
    src.close()


def read_input(name, exe):
    """
    Read input from stdin and send to process
    :param name:
    :param process: process to send input to
    :return:
    """
    global needPassword
    debug ("starting reader for %s" % name )
    while not finished:
        if needPassword:
            needPassword = False
            if sys.stdin.isatty():
                cred = getpass.getpass()
            else:
                cred = sys.stdin.readline().rstrip()
            exe.stdin.write(cred + "\n")

def debug(text):
    if DEBUG: print('[DEBUG] ' + text)


def error(text):
    print('[ERROR] ' + text)
    sys.stdout.flush()

def info(text):
    print(text)
    sys.stdout.flush()

def out(toStdErr, text) :
    """
    Write to one of the system output channels.
    This action does not add newlines. If you want that: write them yourself
    :param toStdErr: flag set if stderr is to be the dest
    :param text: text to write.
    :return:
    """
    if toStdErr:
        sys.stderr.write(text)
    else:
        sys.stdout.write(text)

def flush(toStdErr) :
    """
    Flush the output stream
    :param toStdErr: flag set if stderr is to be the dest
    :return:
    """
    if toStdErr:
        sys.stderr.flush()
    else:
        sys.stdout.flush()

def read(pipe, line):
    """
    read a char, append to the listing if there is a char that is not \n
    :param pipe: pipe to read from
    :param line: line being built up
    :return: (the potentially updated line, flag indicating newline reached)
    """

    c = pipe.read(1)
    if c != "":
        o = c.decode('utf-8')
        if o != '\n':
            line += o
            return line, False
        else:
            return line, True
    else:
        return line, False


def writePid(rest_pid_file, process):
    f = open(rest_pid_file, 'w')
    f.write(str(process.pid))
    f.close()



def exist_pid(pid):
    if  ON_POSIX:
        #check if process id exist in the current process table
        #See man 2 kill - Linux man page for info about the kill(pid,0) system function
        try:
            os.kill(pid, 0)
        except OSError as e :
            return e.errno == errno.EPERM
        else:
            return True

    elif IS_WINDOWS:
        #The os.kill approach does not work on Windows with python 2.7
        #the output from tasklist command is searched for the process id
        pidStr = str(pid)
        command='tasklist /fi  "pid eq %s"' % pidStr
        sub_process=subprocess.Popen(command, stdout = subprocess.PIPE, shell=False)
        sub_process.communicate()
        output = subprocess.check_output(command)
        output=split(" *",output)
        for line in output:
            if pidStr in line:
                return True
        return False
    #os other than nt or posix - not supported - need to delete the file to restart server if pid no longer exist
    return True

def wait_for_shutdown(pid, msg, wait):
    count = 0
    sys.stdout.write(msg)
    while exist_pid(pid):
        sys.stdout.write('.')
        sys.stdout.flush()
        sleep(1)
        if count > wait:
            break
        count = count + 1

    sys.stdout.write('\n')


def get_topics_to_create(confdir):
    confdir = os.path.join(confdir, CONF_FILE)
    topic_list = getConfig(confdir, TOPICS_TO_CREATE)
    if topic_list is not None:
        topics = topic_list.split(",")
    else:
        topics = [getConfigWithDefault("atlas.notification.hook.topic.name", "ATLAS_HOOK"), getConfigWithDefault("atlas.notification.entities.topic.name", "ATLAS_ENTITIES")]
    return topics



def get_rest_url_port(confdir):
    port = None
    if '-port' in sys.argv:
        port = sys.argv[sys.argv.index('-port')+1]

    if port is None:
        confdir = os.path.join(confdir, CONF_FILE)
        enable_tls = getConfig(confdir, REST_ENABLE_TLS)
        if enable_tls is not None and enable_tls.lower() == 'true':
            port = getConfigWithDefault(confdir, REST_HTTPS_PORT, DEFAULT_REST_HTTPS_PORT)
        else:
            port = getConfigWithDefault(confdir, REST_HTTP_PORT, DEFAULT_REST_HTTP_PORT)

    print("Starting REST server on port: %s" % port)
    return port

def get_rest_url_host(confdir):
    confdir = os.path.join(confdir, CONF_FILE)
    host = getConfigWithDefault(confdir, REST_SERVER_BIND_ADDRESS, DEFAULT_REST_SERVER_HOST)
    if (host == '0.0.0.0'):
        host = DEFAULT_REST_SERVER_HOST
    print("\nStarting Rest server on host: %s" % host)
    return host

def wait_for_startup(confdir, wait):
    count = 0
    host = get_rest_url_host(confdir)
    port = get_rest_url_port(confdir)
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((host, int(port)))
            s.close()
            break
        except Exception as e:
            # Wait for 1 sec before next ping
            sys.stdout.write('.')
            sys.stdout.flush()
            sleep(1)

        if count > wait:
            s.close()
            break

        count = count + 1

    sys.stdout.write('\n')


def server_already_running(pid):
    print("Rest server is already running under process %s" % pid)
    sys.exit()

def server_pid_not_running(pid):
    print("The Server is no longer running with pid %s" %pid)


def grep(file, value):
    for line in open(file).readlines():
        if re.match(value, line):
           return line
    return None

def getConfig(file, key):
    key = key + "\s*="
    for line in open(file).readlines():
        if re.match(key, line):
            return line.split('=')[1].strip()
    return None

def getConfigWithDefault(file, key, defaultValue):
    value = getConfig(file, key)
    if value is None:
        value = defaultValue
    return value

def isCygwin():
    return platform.system().startswith("CYGWIN")

def isCygwin():
    return platform.system().startswith("CYGWIN")

# Convert the specified cygwin-style pathname to Windows format,
# using the cygpath utility.  By default, path is assumed
# to be a file system pathname.  If isClasspath is True,
# then path is treated as a Java classpath string.
def convertCygwinPath(path, isClasspath=False):
    if (isClasspath):
        cygpathArgs = ["cygpath", "-w", "-p", path]
    else:
        cygpathArgs = ["cygpath", "-w", path]
    windowsPath = subprocess.Popen(cygpathArgs, stdout=subprocess.PIPE).communicate()[0]
    windowsPath = windowsPath.strip()
    return windowsPath

def get_java_version():
    try:
        output = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode()
        if "version" in output:
            version_line = output.splitlines()[0]
            version_str = version_line.split('"')[1]
            if version_str.startswith("1."):
                return version_str.split('.')[1]
            else:
                return version_str.split('.')[0]
    except Exception:
        return "unknown"

def get_expected_jvm_opts(java_version):
    jvm_opts = []

    try:
        if java_version != "unknown" and int(java_version) >= 9:
            jvm_opts.extend([
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
                "--add-opens=java.base/java.net=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED"
            ])
    except Exception as e:
        print("Warning: Invalid Java version '{}': {}".format(java_version, e))

    return jvm_opts



