FROM openjdk:11

COPY Java-FlightServer-1.0-SNAPSHOT-all.jar /root
COPY logging.xml /root
COPY run_servers.sh /root
COPY run_client.sh /root
ADD classes.tar.gz /root

#CMD ["sleep", "365d"]
CMD ["/root/run_servers.sh"]
