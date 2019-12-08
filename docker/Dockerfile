FROM openjdk:8-alpine

RUN apk --update add wget tar bash python3

RUN ln /usr/bin/python3 /usr/bin/python

# https://issues.apache.org/jira/browse/SPARK-26995
# Running Spark in Docker image with Alpine Linux 3.9.0 throws errors when using snappy
# Alpine doesn't use glibc so we need to install libc6-compat.
# Or use: ENV LD_LIBRARY_PATH /lib64
RUN apk update && \
  apk add --no-cache libc6-compat && \
  ln -s /lib/libc.musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2
 
RUN wget http://apache.mirrors.tds.net/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz

RUN tar -xzf spark-2.4.4-bin-hadoop2.7.tgz && \
  mv spark-2.4.4-bin-hadoop2.7 /spark && \
  rm spark-2.4.4-bin-hadoop2.7.tgz

COPY start-master.sh /start-master.sh

COPY start-worker.sh /start-worker.sh
