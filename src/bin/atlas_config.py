#!/usr/bin/env python

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

import os
import platform
import subprocess
from threading import Thread
import sys
import time
import errno

LIB = "lib"
CONF = "conf"
LOG="logs"
WEBAPP="server" + os.sep + "webapp"
DATA="data"
ENV_KEYS = ["JAVA_HOME", "METADATA_OPTS", "METADATA_LOG_DIR", "METADATA_PID_DIR", "METADATA_CONF", "METADATACPPATH", "METADATA_DATA_DIR", "METADATA_HOME_DIR", "METADATA_EXPANDED_WEBAPP_DIR"]
METADATA_CONF = "METADATA_CONF"
METADATA_LOG = "METADATA_LOG_DIR"
METADATA_PID = "METADATA_PID_DIR"
METADATA_WEBAPP = "METADATA_EXPANDED_WEBAPP_DIR"
METADATA_OPTS = "METADATA_OPTS"
METADATA_DATA = "METADATA_DATA_DIR"
METADATA_HOME = "METADATA_HOME_DIR"
IS_WINDOWS = platform.system() == "Windows"
ON_POSIX = 'posix' in sys.builtin_module_names
DEBUG = False

def scriptDir():
    """
    get the script path
    """
    return os.path.dirname(os.path.realpath(__file__))

def metadataDir():
    home = os.path.dirname(scriptDir())
    return os.environ.get(METADATA_HOME, home)

def libDir(dir) :
    return os.path.join(dir, LIB)

def confDir(dir):
    localconf = os.path.join(dir, CONF)
    return os.environ.get(METADATA_CONF, localconf)

def logDir(dir):
    localLog = os.path.join(dir, LOG)
    return os.environ.get(METADATA_LOG, localLog)

def pidFile(dir):
    localPid = os.path.join(dir, LOG)
    return os.path.join(os.environ.get(METADATA_PID, localPid), 'atlas.pid')

def dataDir(dir):
    data = os.path.join(dir, DATA)
    return os.environ.get(METADATA_DATA, data)

def webAppDir(dir):
    webapp = os.path.join(dir, WEBAPP)
    return os.environ.get(METADATA_WEBAPP, webapp)

def expandWebApp(dir):
    webappDir = webAppDir(dir)
    webAppMetadataDir = os.path.join(webappDir, "atlas")
    d = os.sep
    if not os.path.exists(os.path.join(webAppMetadataDir, "WEB-INF")):
        try:
            os.makedirs(webAppMetadataDir)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise e
            pass
        os.chdir(webAppMetadataDir)
        jar(os.path.join(metadataDir(), "server", "webapp", "atlas.war"))

def dirMustExist(dirname):
    if not os.path.exists(dirname):
        os.mkdir(dirname)
    return dirname

def executeEnvSh(confDir):
    envscript = '%s/atlas-env.sh' % confDir
    if not IS_WINDOWS and os.path.exists(envscript):
        envCmd = 'source %s && env' % envscript
        command = ['bash', '-c', envCmd]

        proc = subprocess.Popen(command, stdout = subprocess.PIPE)

        for line in proc.stdout:
            (key, _, value) = line.strip().partition("=")
            if key in ENV_KEYS:
                os.environ[key] = value

        proc.communicate()

def java(classname, args, classpath, jvm_opts_list, logdir=None):
    java_home = os.environ.get("JAVA_HOME", None)
    if java_home:
        prg = os.path.join(java_home, "bin", "java")
    else:
        prg = which("java")

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

def runProcess(commandline, logdir=None):
    """
    Run a process
    :param commandline: command line
    :return:the return code
    """
    global finished
    debug ("Executing : %s" % commandline)
    timestr = time.strftime("metadata.%Y%m%d-%H%M%S")
    stdoutFile = None
    stderrFile = None
    if logdir:
        stdoutFile = open(os.path.join(logdir, timestr + ".out"), "w")
        stderrFile = open(os.path.join(logdir,timestr + ".err"), "w")
    return subprocess.Popen(commandline, stdout=stdoutFile, stderr=stderrFile)

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
    if DEBUG: print '[DEBUG] ' + text


def error(text):
    print '[ERROR] ' + text
    sys.stdout.flush()

def info(text):
    print text
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

def writePid(metadata_pid_file, process):
    f = open(metadata_pid_file, 'w')
    f.write(str(process.pid))
    f.close()


