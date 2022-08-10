## Create Namespace in k8
kubectl -n kafka apply -f kafka_namespace.yml
## Install strimzi operator in k8
kubectl -n kafka apply -f strimzi.yml
## Create kafka cluster in k8
kubectl -n kafka apply -f kafka-cluster.yml

## Wait for k8 to provision all the kafka cluster resources
kubectl wait kafka/desponge-cluster --for=condition=Ready --timeout=300s -n kafka 

## Build and push the Strimzi kafka connect image to a container registry
sh kafka-debezium-connector-image.sh

## Set postgres credentials inside a property file to store them as k8 secret
cat <<EOF > debezium-postgresql-credentials.properties
postgresql_username: postgres
postgresql_password: 1qazxsw2
EOF

kubectl -n kafka create secret generic postgres-credentials \
  --from-file=debezium-postgresql-credentials.properties --dry-run=client -o yaml | kubectl apply -f -

rm debezium-postgresql-credentials.properties


## Create the kafka connect cluster
kubectl -n kafka apply -f kafka-connect-cluster.yml

## When using MINIKUBE open an ssh tunnel loadbalancer using the command "minikube tunnel"