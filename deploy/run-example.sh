#!/bin/bash

# CONFIGURATION

EXAMPLE_NAME='HelloForefinger'
VM_ADDRESS='192.168.137.137'

VM_USER='hadoop'
ARTIFACTS_HOME='../out/artifacts'
EXMAPLE_PACKAGE='com.euyuil.forefinger.example'

# GENERATED

JAR_FILE="$ARTIFACTS_HOME/${EXAMPLE_NAME}_jar/$EXAMPLE_NAME.jar"
CLASS_NAME="$EXMAPLE_PACKAGE.$EXAMPLE_NAME"
VM="$VM_USER@$VM_ADDRESS"
HADOOP_COMMAND="source /etc/bashrc && start-all.sh && jps && hadoop dfsadmin -safemode leave && hadoop jar ~/$EXAMPLE_NAME.jar $CLASS_NAME"

echo "Jar file: $JAR_FILE"
echo "Class name: $CLASS_NAME"
echo "Virtual machine: $VM"
echo "Hadoop command: $HADOOP_COMMAND"

scp $JAR_FILE "$VM:~"
ssh $VM $HADOOP_COMMAND

