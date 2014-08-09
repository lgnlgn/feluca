#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin/.."; pwd`

PROJECT_HOME=$bin

JAVA_HOME=${JAVA_HOME:-/usr/local/webserver/jdk1.6.0_23}
PROJECT_CLASS=com.meiliwan.search.spell.server.SpellServer

export JAVA_HOME

CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar:${PROJECT_HOME}/conf

for f in ${bin}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

export CLASSPATH

PROJECT_DAEMON_PIDFILE=$bin/pids/cr.pid
DAEMON_LOG_FILE=$bin/logs/cr.log

JAVA_EXEC=$JAVA_HOME/bin/java
JAVA_OPTS="-server -Xmx2048m -Xms2048m -Djava.net.preferIPv4Addresses=true -Djava.net.preferIPv6Addresses=false"




function start_server() {
  pid_file=$1
  debug=$2
  debug_port=$3
  rotate_log_file=$4
  log_file=$5
  class=$6	
  if [ -f ${pid_file} ]; then
     echo "please stop `cat ${pid_file}` first".
      exit 1
  fi
  DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=${debug_port},suspend=n"
  if [[ "$debug" == "true" ]]; then
    JAVA_OPTS="$JAVA_OPTS $DEBUG_OPTS"
  fi
  
  if [[ "x${rotate_log_file}" == "xtrue" ]]; then
    file_name=${log_file}.`date "+%Y-%m-%d"`
    /bin/dd if="${log_file}" of="${file_name}" conv=notrunc oflag=append
  fi
  nohup $JAVA_EXEC $JAVA_OPTS -classpath "$CLASSPATH" $class > ${log_file} 2>&1 < /dev/null &
  pid=$!
  echo $pid > ${pid_file}
  sleep 1
  ps -ef | grep $pid
  echo "$class start successful"
}

function stop_server() {
  class=$1
  pid_file=$2
  pid=`cat "${pid_file}"`
  kill $pid
  sleep 1
  ps -ef | grep $pid
  rm -rf ${pid_file}
  echo "$class stop successful"
}
