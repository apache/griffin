FROM bhlx3lyx7/griffin_env:0.1.0
MAINTAINER org.apache.griffin

WORKDIR /root

# files into hdfs
ADD prep /root
RUN chmod 755 prepare.sh
RUN /etc/bootstrap.sh && ./prepare.sh

# entrypoint
ADD bootstrap-all.sh /etc/
RUN chmod 755 /etc/bootstrap-all.sh

ENTRYPOINT ["/etc/bootstrap-all.sh"]
