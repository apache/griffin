FROM ubuntu:14.04
MAINTAINER org.apache.griffin

#install wget, ssh, unzip
RUN apt-get update && apt-get install wget -y && apt-get install openssh-client openssh-server -y && apt-get install unzip -y
RUN mkdir /apache 
WORKDIR /apache
ADD prep /apache

EXPOSE 2122

#java
RUN ./software-install.sh
ADD conf /apache/conf
RUN ./dir.sh && ./software-config.sh
ENV JAVA_HOME /apache/jdk
ENV HADOOP_HOME /apache/hadoop
ENV HADOOP_INSTALL $HADOOP_HOME
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_COMMON_HOME $HADOOP_HOME
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV YARN_HOME $HADOOP_HOME
ENV HADOOP_COMMON_LIB_NATIVE_DIR $HADOOP_HOME/lib/native
ENV SCALA_HOME /apache/scala
ENV SPARK_HOME /apache/spark
ENV HIVE_HOME /apache/hive
ENV HADOOP_USER_CLASSPATH_FIRST true
ENV LIVY_HOME /apache/livy

ENV PATH $JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin:$HIVE_HOME/bin:$LIVY_HOME/bin

EXPOSE 3306
EXPOSE 9000 10020
EXPOSE 50010 50020 50070 50075 50090
EXPOSE 19888
EXPOSE 8030 8031 8032 8033 8040 8042 8088
EXPOSE 49707
EXPOSE 9083 27017 6066
EXPOSE 8080
EXPOSE 8998
EXPOSE 9200

#ssh without password
ENV HOME /root
WORKDIR /root
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa && cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
ADD ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config && chown root:root /root/.ssh/config
RUN sed  -i "/^[^#]*UsePAM/ s/.*/#&/"  /etc/ssh/sshd_config && echo "UsePAM no" >> /etc/ssh/sshd_config && echo "Port 2122" >> /etc/ssh/sshd_config

#CURL
RUN apt-get install curl -y

#mysql init
RUN DEBIAN_FRONTEND=noninteractive apt-get install mysql-server -y && DEBIAN_FRONTEND=noninteractive apt-get install libmysql-java -y
RUN ln -s /usr/share/java/mysql-connector-java.jar $HIVE_HOME/lib/mysql-connector-java.jar && ln -s /usr/share/java/mysql.jar $HIVE_HOME/lib/mysql.jar
RUN cd /apache/conf/mysql && cp bind_0.cnf /etc/mysql/conf.d/bind_0.cnf && ./mysql-init.sh

#bootstrap
ADD bootstrap.sh /etc/
RUN chmod 755 /etc/bootstrap.sh

#initial
RUN hdfs namenode -format
RUN /etc/bootstrap.sh && /apache/hdfs_file.sh && rm /apache/*.sh

#bootstrap-all
ADD bootstrap-all.sh /etc/
RUN chmod 755 /etc/bootstrap-all.sh

ENTRYPOINT ["/etc/bootstrap-all.sh"]
