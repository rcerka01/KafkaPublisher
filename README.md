# Mars Rover

### Start Kafka Docker with Zookeeper

```
docker pull bitnami/kafka
docker run -d --name kafka-container -p 9092:9092 -p 2181:2181 -e ALLOW_PLAINTEXT_LISTENER=yes bitnami/kafka
```