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

wget $HDURL -O $HDTEMP
mkdir /opt
(cd /opt && tar xzvf $HDTEMP)
ln -s /opt/$HDNAME /opt/hadoop

echo 'export HADOOP_OPTS=-Djava.net.preferIPv4Stack=true' >> /opt/hadoop/conf/hadoop-env.sh

su -c "echo 'export HADOOP_PREFIX=/opt/hadoop' >> ~/.profile" -s /bin/bash $HDUSER
su -c "echo 'export PATH=\$PATH:\$HADOOP_PREFIX/bin' >> ~/.profile" -s /bin/bash $HDUSER
su -c "echo 'export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64' >> ~/.profile" -s /bin/bash $HDUSER
echo 'export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64' >> /opt/hadoop/conf/hadoop-env.sh

su -c "echo 'unalias hfs &> /dev/null' >> ~/.profile" -s /bin/bash $HDUSER
su -c "echo \"alias hfs='hadoop fs'\" >> ~/.profile" -s /bin/bash $HDUSER
su -c "echo 'unalias hls &> /dev/null' >> ~/.profile" -s /bin/bash $HDUSER
su -c "echo \"alias hls='hfs -ls'\" >> ~/.profile" -s /bin/bash $HDUSER

\cp -rf ./conf-single/* /opt/hadoop/conf

chown -R hadoop:hadoop /opt/$HDNAME

su -c "source ~/.profile && hadoop namenode -format" -s /bin/bash $HDUSER
su -c "source ~/.profile && start-all.sh" -s /bin/bash $HDUSER
