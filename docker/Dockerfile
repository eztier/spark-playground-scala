FROM openjdk:8-alpine

RUN apk add --no-cache --virtual .build-deps g++ python3-dev libffi-dev openssl-dev && \
    apk add --no-cache --update wget tar bash nano python3 && \
    pip3 install --upgrade pip setuptools

RUN ln -s /usr/include/locale.h /usr/include/xlocale.h
RUN ln /usr/bin/python3 /usr/bin/python
RUN pip install numpy

RUN wget http://mirror.cc.columbia.edu/pub/software/apache/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz

RUN tar -xzf spark-2.4.3-bin-hadoop2.7.tgz && \
  mv spark-2.4.3-bin-hadoop2.7 /spark && \
  rm spark-2.4.3-bin-hadoop2.7.tgz

COPY start-master.sh /start-master.sh

COPY start-worker.sh /start-worker.sh
