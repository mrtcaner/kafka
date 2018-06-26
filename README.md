# kafka
A repo to toy around with kafka

## Build

docker run --rm -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=127.0.0.1 landoop/fast-data-dev:latest

* To create topic from console
docker exec -it <container_id>  bash

kafka-topics --zookeeper 127.0.0.1:2181 --create -topic test_topic --partitions 3 --replication-factor 1

* To supply some data from console

docker exec -it <container_id>  bash

kafka-console-producer --broker-list 127.0.0.1:9092 --topic test_topic

* To consume data from console

docker exec -it <container_id>  bash

kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic test_topic

* Check more params on https://kafka.apache.org/documentation/