#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

source "$bin"/env.sh

cd $bin
start_server ${PROJECT_DAEMON_PIDFILE} false 40174 true ${DAEMON_LOG_FILE} ${PROJECT_CLASS} 
