#Prerequisites

##Do the following on a load generation node, these instructions are for ubuntu or debian based Linux. For CentOS or MacOS, google the equivalents and keep the versions matched.


###Install Zookeeper
`sudo apt-get install zookeeperd`

###Test Zookeeper
`telnet localhost 2181`  

###Type ruok in telnet, should say imok

###Download Kafka 0.9.0.1 for Scala 2.10
`wget "http://mirror.cc.columbia.edu/pub/software/apache/kafka/0.9.0.1/kafka_2.10-0.9.0.1.tgz" -O kafka.tgz`

###Make Kafka dir and cd to it
`mkdir -p ~/kafka && cd ~/kafka`
`tar -xvzf ../kafka.tgz --strip 1`

###Edit properties
`vi ~/kafka/config/server.properties`
###Add the following to the end of the file
`delete.topic.enable = true`

###Start Kafka
`nohup ~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties > ~/kafka/kafka.log 2>&1 &`

###Install newest SBT
`echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list`
`sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823`
`sudo apt-get update`
`sudo apt-get install sbt`

###Install scala 2.10.6
`sudo apt-get remove scala-library scala`
`wget http://www.scala-lang.org/files/archive/scala-2.10.6.deb`
`sudo dpkg -i scala-2.10.6.deb`
`sudo apt-get update`
`sudo apt-get install scala`

###Install git
`sudo apt-get install git`
