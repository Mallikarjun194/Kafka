FROM ubuntu:latest

#WORKDIR /k-producer
ADD /consumer .
#RUN apt-get update && \
#    apt-get install build-essential librdkafka-dev -y
#EXPOSE 9092:9092
# KAFKA_HOSTS=<SystemIP>:9092
ENV KAFKA_HOSTS=192.168.1.5:9092
CMD ["./consumer/consumer"]