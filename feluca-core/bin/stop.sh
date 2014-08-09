#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
source ${bin}/env.sh
stop_server ${PROJECT_CLASS} ${PROJECT_DAEMON_PIDFILE}
