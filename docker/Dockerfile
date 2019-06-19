FROM openjdk:8-alpine

RUN apk --update add wget tar bash python3

RUN ln /usr/bin/python3 /usr/bin/python

RUN wget http://mirror.cc.columbia.edu/pub/software/apache/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz

RUN tar -xzf spark-2.4.3-bin-hadoop2.7.tgz && \
  mv spark-2.4.3-bin-hadoop2.7 /spark && \
  rm spark-2.4.3-bin-hadoop2.7.tgz

COPY start-master.sh /start-master.sh

COPY start-worker.sh /start-worker.sh
