#!/usr/bin/env bash
sbt clean assembly
cp target/scala-2.12/FlinkHelloWorld-assembly-0.1.jar /opt/flink

cd docker
sudo docker-compose exec taskmanager bash /opt/flink/bin/flink run -c stream.StreamKafka /opt/data/FlinkHelloWorld-assembly-0.1.jar --kafka-servers host:9091 --kafka-username myUser --kafka-password myPass --topic-complex complex --topic-extra extra
