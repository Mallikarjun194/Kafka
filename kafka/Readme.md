

Setup kafka:
https://data-flair.training/blogs/kafka-cluster/ 

https://kafka.apache.org/quickstart 

Steps:

1. Create a topic:
   >> ./bin/kafka-topics.sh --create --topic my-topic --bootstrap-server localhost:9092

2. To describe the topic:
   >> ./bin/kafka-topics.sh --describe --topic my-topic --bootstrap-server localhost:9092

3. Write something to kafka topic:
   >> ./bin/kafka-console-producer.sh --topic my-topic --bootstrap-server localhost:9092

4. Read from kafka:
   >> ./bin/kafka-console-consumer.sh --topic my-topic --from-beginning --bootstrap-server localhost:9092