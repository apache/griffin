FROM griffin-base-env:latest
MAINTAINER com.ebay.oss


#input files
ADD script/*.sh $GRIFFIN_HOME/
ADD ./*.sh $GRIFFIN_HOME/
RUN mkdir $GRIFFIN_HOME/tmp $GRIFFIN_HOME/temp $GRIFFIN_HOME/log
RUN chmod -R 755 $GRIFFIN_HOME

#add db
RUN mkdir /db
ADD db /db

#remove tomcat webapps
ENV APACHE_HOME /apache/tomcat
RUN rm -rf $APACHE_HOME/webapps/*

#prepare data
WORKDIR $GRIFFIN_HOME
RUN ./hadoop-start.sh && ./pre-start.sh && ./hadoop-end.sh && rm hadoop-start.sh hadoop-end.sh pre-start.sh

#download resources
RUN ./download-resources.sh && rm download-resources.sh

#entrypoint
ENTRYPOINT ["./start-up.sh"]
