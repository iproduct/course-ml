FROM openjdk:8u292-slim-buster

WORKDIR /opt

ARG kafkaversion=3.2.0
ARG scalaversion=2.13

ENV KAFKA_CONTAINER_HOST_NAME=kafka-broker-03
ENV BOOTSTRAP_SERVERS=localhost:9093,localhost:8093
ENV BROKER_ID=1
ENV BROKER_EXTERNAL_PORT=9093
ENV BROKER_EXTERNAL_PORT_SSL=8093
ENV CONTROLLER_QUORUM_VOTERS=1@localhost:19092
ENV KAFKA_CREATE_TOPICS=temperature
ENV KAFKA_PARTITIONS_PER_TOPIC=12
ENV KAFKA_PARTITIONS_REPLICATION_FACTOR=1
ENV MIN_IN_SYNC_REPLICAS=1
#ENV KAFKA_OPTS="-javaagent:./jmx_prometheus_javaagent-0.17.0.jar=8282:./config/kafka_broker.yml -Djava.security.auth.login.config=/opt/kafka/kafka_server_jaas.conf"
ENV KAFKA_OPTS="-javaagent:./jmx_prometheus_javaagent-0.17.0.jar=8282:./config/kafka_broker.yml"
ENV KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
#ENV JMX_PORT=12345
#ENV BROKER_IP=172.28.0.2



RUN apt update \
    && apt install -y --no-install-recommends wget

RUN wget https://mirrors.ocf.berkeley.edu/apache/kafka/${kafkaversion}/kafka_${scalaversion}-${kafkaversion}.tgz -O kafka.tgz \
    && tar xvzf kafka.tgz \
    && mv kafka_${scalaversion}-${kafkaversion} kafka

WORKDIR /opt/kafka

COPY ./jmx_prometheus_javaagent-0.17.0.jar .
COPY ./configs/prometheus-jmx-agent-config/kafka_broker.yml ./config

COPY ./configs/server.properties ./config/kraft
COPY ./*.sh /opt/kafka/
COPY ./security/* ./

#EXPOSE 9092 9093 8282 8093 8092
EXPOSE 9092 9093

ENTRYPOINT ["bash", "./docker-entrypoint.sh" ]
