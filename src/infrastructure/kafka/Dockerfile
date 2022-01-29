FROM quay.io/strimzi/kafka:0.27.1-kafka-3.0.0
USER root:root
RUN mkdir -p /opt/kafka/plugins/debezium
COPY ./debezium-connector-postgres/ /opt/kafka/plugins/debezium/
USER 1001
