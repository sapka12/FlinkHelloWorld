#!/usr/bin/env bash
rm /opt/flink/cab-flink-out.txt
rm /opt/flink/cab-flink-out-popular.txt
rm /opt/flink/output-avgpassangers.txt
rm /opt/flink/output-avgpassangersperdriver.txt


sbt clean assembly
cp target/scala-2.12/FlinkHelloWorld-assembly-0.1.jar /opt/flink

cd docker
sudo docker-compose exec taskmanager bash /opt/flink/bin/flink run -c batch.CabFlink /opt/data/FlinkHelloWorld-assembly-0.1.jar --input /opt/data/cab-flink.txt --output /opt/data/cab-flink-out.txt --output-popular /opt/data/cab-flink-out-popular.txt --output-avgpassangers /opt/data/cab-flink-out-avgpassangers.txt --output-avgpassangersperdriver /opt/data/cab-flink-out-avgpassangersperdriver.txt
