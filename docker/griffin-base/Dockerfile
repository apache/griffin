FROM sequenceiq/spark:1.6.0
MAINTAINER com.ebay.oss

#add user
RUN rpm -e cracklib-dicts --nodeps && yum install -y cracklib-dicts
ADD user/*.sh /root/
WORKDIR /root
RUN ./adduser.sh griffin griffin && ./sudouser.sh griffin && rm *.sh
ENV GRIFFIN_HOME /home/griffin

#set java environment variables
ENV JAVA_HOME /usr/java/latest
ENV PATH $JAVA_HOME/bin:$PATH

#install wget
RUN yum install -y wget

#enter /apache
RUN mkdir /apache

#install hive 1.2.1 and set environment variables
RUN cd /apache && wget https://www.apache.org/dist/hive/hive-1.2.1/apache-hive-1.2.1-bin.tar.gz && tar -xvf apache-hive-1.2.1-bin.tar.gz && ln -s apache-hive-1.2.1-bin hive
ENV HIVE_HOME /apache/hive
ENV PATH $HIVE_HOME/bin:$PATH

#running HiveServer2 and Beeline
ENV HADOOP_USER_CLASSPATH_FIRST true
RUN rm /usr/local/hadoop-2.6.0/share/hadoop/yarn/lib/jline-0.9.94.jar

#mkdir
ADD griffin $GRIFFIN_HOME
RUN chmod -R 755 $GRIFFIN_HOME

#install tomcat 7
RUN cd /apache && wget https://www.apache.org/dist/tomcat/tomcat-7/v7.0.73/bin/apache-tomcat-7.0.73.tar.gz && tar -xvf apache-tomcat-7.0.73.tar.gz && ln -s apache-tomcat-7.0.73 tomcat
ADD config/tomcat /etc/init.d/
RUN chmod 755 /etc/init.d/tomcat
ENV TOMCAT_HOME /apache/tomcat
ENV PATH $TOMCAT_HOME/bin:$PATH

#install mongodb
ADD config/mongodb-org-3.2.repo /etc/yum.repos.d/
RUN yum install -y mongodb-org mongodb-org-server mongodb-org-shell mongodb-org-mongos mongodb-org-tools && yum clean all

#expose ports
EXPOSE 8080 27017 6066 2122 9083 3306

#env
ENV HADOOP_HOME /usr/local/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin

#input hadoop data
WORKDIR $GRIFFIN_HOME
RUN ./hadoop-start.sh && ./pre-start.sh && ./hd-before-hive.sh && ./hd-after-hive.sh && ./hd-test-json.sh && ./hadoop-end.sh

#install mysql
ADD config/mysql_* $GRIFFIN_HOME/
RUN chmod 755 $GRIFFIN_HOME/mysql_*
RUN yum install -y mysql-server && yum install -y mysql-connector-java && ln -s /usr/share/java/mysql-connector-java.jar $HIVE_HOME/lib/mysql-connector-java.jar

#configure hive metastore as remote mode
ADD config/hive-site.xml $HIVE_HOME/conf/
ADD config/hive-site.xml $SPARK_HOME/conf/
RUN chmod 664 $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml

#prepare env data
RUN ./hadoop-start.sh && ./hive-init.sh && ./hadoop-end.sh && rm hadoop-start.sh pre-start.sh hd-before-hive.sh hd-after-hive.sh hd-test-json.sh hadoop-end.sh hive-init.sh

#modify spark webui port
ADD config/spark-conf-mod.sh $GRIFFIN_HOME/
RUN ./spark-conf-mod.sh && rm spark-conf-mod.sh

#edit profile
ADD user/env.txt $GRIFFIN_HOME/
RUN cat ./env.txt >> /etc/profile

#input start-up.sh
ADD start-up.sh $GRIFFIN_HOME/

#entry point
ENTRYPOINT ["./start-up.sh"]
