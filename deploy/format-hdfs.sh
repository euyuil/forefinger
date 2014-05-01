#!/bin/bash

# CONFIGURATION

VM_ADDRESS='192.168.137.137'

VM_USER='hadoop'

# GENERATED

VM="$VM_USER@$VM_ADDRESS"
HADOOP_COMMAND="source /etc/bashrc && stop-all.sh && rm -rf /opt/hadoop/tmp/dfs && hadoop namenode -format -force && start-all.sh && jps"

echo "Virtual machine: $VM"
echo "Hadoop command: $HADOOP_COMMAND"

ssh $VM $HADOOP_COMMAND

