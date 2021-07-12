FROM openjdk:11

RUN apt-get update \
    && apt-get install -y time

COPY logging.xml /root
COPY *.jar /root/
COPY *.sh /root/

CMD ["sleep", "365d"]
