#!/bin/bash

_term() {
  echo "🚨 Termination signal received..."
  kill -TERM "$child" 2>/dev/null
}

trap _term SIGINT SIGTERM

properties_file=/opt/kafka/config/kraft/server.properties
#jmx_properties_file=/opt/kafka/config/kraft/jmx_local.config
kafka_addr="${KAFKA_CONTAINER_HOST_NAME}:9092"
kafka_addr_ssl="${KAFKA_CONTAINER_HOST_NAME}:8092"

echo "==> Applying environment variables..."

#if [ -z $KAFKA_CONTAINER_HOST_NAME ]; then
#    echo "listeners=CONTROLLER://:19092,EXTERNAL://localhost:${BROKER_EXTERNAL_PORT}" >> $properties_file;
#    echo "advertised.listeners=EXTERNAL://localhost:${BROKER_EXTERNAL_PORT}" >> $properties_file;
#    echo "inter.broker.listener.name=EXTERNAL" >> $properties_file;
#    echo "listener.security.protocol.map=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT" >> $properties_file;
#else
#echo "listeners=CONTROLLER://:19092,INTERNAL://:9092,EXTERNAL://:${BROKER_EXTERNAL_PORT},SASL_SSL_INTERNAL://:8092,SASL_SSL://:${BROKER_EXTERNAL_PORT_SSL}" >>$properties_file
echo "listeners=CONTROLLER://:19092,INTERNAL://:9092,EXTERNAL://:${BROKER_EXTERNAL_PORT}" >>$properties_file
#echo "advertised.listeners=INTERNAL://${KRAFT_CONTAINER_HOST_NAME}:9092,EXTERNAL://localhost:${BROKER_EXTERNAL_PORT},SASL_SSL_INTERNAL://${KRAFT_CONTAINER_HOST_NAME}:8092,SASL_SSL://localhost:${BROKER_EXTERNAL_PORT_SSL}" >>$properties_file
echo "advertised.listeners=INTERNAL://${KRAFT_CONTAINER_HOST_NAME}:9092,EXTERNAL://localhost:${BROKER_EXTERNAL_PORT}" >>$properties_file
echo "inter.broker.listener.name=EXTERNAL" >>$properties_file
#echo "listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,SASL_SSL_INTERNAL:SASL_SSL,SASL_SSL:SASL_SSL" >>$properties_file
echo "listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT" >>$properties_file
#fi
echo "broker.id=${BROKER_ID}" >>$properties_file
echo "node.id=${BROKER_ID}" >>$properties_file
echo "controller.quorum.voters=${CONTROLLER_QUORUM_VOTERS}" >>$properties_file
echo "controller.listener.names=CONTROLLER" >>$properties_file
echo "min.insync.replicas=${MIN_IN_SYNC_REPLICAS}" >>$properties_file
#echo "auto.create.topics.enable=false" >>$properties_file

echo "==> ✅ Enivronment variables applied."

echo "==> Setting up Kafka storage..."
#export suuid=$(./bin/kafka-storage.sh random-uuid);
export suuid="1-wTPNMYQkinQ7KUbCZi9g"
./bin/kafka-storage.sh format -t $suuid -c ./config/kraft/server.properties
echo "==> ✅ Kafka storage setup."

echo "==> Starting Kafka server..."
./bin/kafka-server-start.sh ./config/kraft/server.properties &
child=$!
unset KAFKA_OPTS

echo "==> ✅ Kafka server started."

if [ -z $KAFKA_CREATE_TOPICS ]; then
  echo "==> No topic requested for creation."
else
  echo "==> Creating topics..."
  ./wait-for-it.sh $kafka_addr

  pc=1
  if [ $KAFKA_PARTITIONS_PER_TOPIC ]; then
    pc=$KAFKA_PARTITIONS_PER_TOPIC
  fi

  for i in $(echo $KAFKA_CREATE_TOPICS | sed "s/,/ /g"); do
    ./bin/kafka-topics.sh --create --topic "$i" --partitions "$pc" --replication-factor "${KAFKA_PARTITIONS_REPLICATION_FACTOR}" --bootstrap-server "${BOOTSTRAP_SERVERS}"
  done
  echo "==> ✅ Requested topics created."
fi

wait "$child"
