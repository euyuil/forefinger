#!/bin/bash

HDNAME='hadoop-1.2.1'
HDURL="http://mirrors.cnnic.cn/apache/hadoop/common/$HDNAME/$HDNAME.tar.gz"
HDTEMP="/tmp/$HDNAME.tar.gz"
HDUSER='hadoop'

apt-get update
apt-get upgrade -y

apt-get install -y openssh-server \
                   wget \
                   curl \
                   rsync \
                   git \
                   vim \
                   openjdk-7-jdk

useradd --create-home --user-group --shell /bin/bash $HDUSER
su -c "ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''" -s /bin/bash $HDUSER
su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" -s /bin/bash $HDUSER
su -c "ssh-keyscan -t ecdsa localhost >> ~/.ssh/known_hosts" -s /bin/bash $HDUSER
su -c "ssh-keyscan -t ecdsa 127.0.0.1 >> ~/.ssh/known_hosts" -s /bin/bash $HDUSER

wget $HDURL -O $HDTEMP
mkdir /opt
(cd /opt && tar xzvf $HDTEMP)
ln -s /opt/$HDNAME /opt/hadoop

echo 'export HADOOP_OPTS=-Djava.net.preferIPv4Stack=true' >> /opt/hadoop/conf/hadoop-env.sh

echo 'export HADOOP_PREFIX=/opt/hadoop' >> /etc/bashrc
echo 'export PATH=$PATH:$HADOOP_PREFIX/bin' >> /etc/bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64' >> /etc/bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64' >> /opt/hadoop/conf/hadoop-env.sh

echo "alias hfs='hadoop fs'"            >> /etc/bashrc
echo "alias hls='hfs -ls'"              >> /etc/bashrc
echo "alias hcat='hfs -cat'"            >> /etc/bashrc
echo "alias hchgrp='hfs -chgrp'"        >> /etc/bashrc
echo "alias hchown='hfs -chown'"        >> /etc/bashrc
echo "alias hcpfl='hfs -copyFromLocal'" >> /etc/bashrc
echo "alias hcptl='hfs -copyToLocal'"   >> /etc/bashrc
echo "alias hcp='hfs -cp'"              >> /etc/bashrc
echo "alias hmv='hfs -mv'"              >> /etc/bashrc
echo "alias hget='hfs -get'"            >> /etc/bashrc
echo "alias hput='hfs -put'"            >> /etc/bashrc

su -c "echo 'source /etc/bashrc' >> ~/.bashrc" -s /bin/bash $HDUSER

\cp -rf ./conf-single/* /opt/hadoop/conf

chown -R hadoop:hadoop /opt/$HDNAME

su -c "source /etc/bashrc && hadoop namenode -format -nonInteractive -force" -s /bin/bash $HDUSER
su -c "source /etc/bashrc && start-all.sh" -s /bin/bash $HDUSER
su -c "source /etc/bashrc && jps /" -s /bin/bash $HDUSER
netstat -plten | grep java
su -c "source /etc/bashrc && hadoop fs -ls /" -s /bin/bash $HDUSER
