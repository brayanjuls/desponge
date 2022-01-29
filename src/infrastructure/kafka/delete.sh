## Create the kafka connect cluster
kubectl -n kafka delete -f kafka-connect-cluster.yml

kubectl -n kafka delete secret postgres-credentials 

## Create kafka delete in k8
kubectl -n kafka delete -f kafka-cluster.yml

## Wait for k8 to provision all the kafka cluster resources
kubectl wait kafka/desponge-cluster --for=condition=Terminating --timeout=300s -n kafka 

## Install strimzi operator in k8
kubectl -n kafka delete -f strimzi.yml

## Create Namespace in k8
kubectl -n kafka delete -f kafka_namespace.yml

# Delete zookeeper and broker pv from cluster