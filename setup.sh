#!/bin/bash

set -o errexit
set -o nounset

launch_node (){
  echo "Launching node $1"
  echo "Press any key to continue..."
  read -n 1
  mvn exec:java -D exec.mainClass=com.isikun.firat.dht.simplified.Main -DconfigFile=config$1.properties &
  sleep 3
}

mvn clean install

launch_node 1
launch_node 2
#launch_node 3
#launch_node 4

