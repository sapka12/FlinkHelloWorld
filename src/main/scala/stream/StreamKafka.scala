package stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


object StreamKafka {


  object ComplexEvent {
    def apply(rawData: String): ComplexEvent = {
      val data = rawData.split(",")
      ComplexEvent(data(0).toLong, data(1).toInt, data(2), List())
    }
  }


  object Extra {
    def apply(rawData: String): Extra = {
      val data = rawData.split(",")
      Extra(data(0).toLong, data(1).toInt, data(2))
    }
  }


  case class ComplexEvent(time: Long, id: Int, description: String, extras: List[Extra])
  case class Extra(time: Long, eventId: Int, data: String)


  def props(kafkaServers: String, username: String, password: String, serializer: String, deserializer: String) = {
    val jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
    val jaasCfg = String.format(jaasTemplate, username, password)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaServers)
    properties.put("group.id", username + "-consumer")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "1000")
    properties.put("auto.offset.reset", "earliest")
    properties.put("session.timeout.ms", "30000")
    properties.put("key.deserializer", deserializer)
    properties.put("value.deserializer", deserializer)
    properties.put("key.serializer", serializer)
    properties.put("value.serializer", serializer)
    properties.put("security.protocol", "SASL_SSL")
    properties.put("sasl.mechanism", "SCRAM-SHA-256")
    properties.put("sasl.jaas.config", jaasCfg)
    properties
  }


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val kafkaServers = params.get("kafka-servers")
    val username = params.get("kafka-username")
    val password = params.get("kafka-password")

    val topicComplexEvent = params.get("topic-complex")
    val topicExtra = params.get("topic-extra")

    val schema = new SimpleStringSchema
    val serializer = classOf[StringSerializer].getName
    val deserializer = classOf[StringDeserializer].getName
    val properties = props(kafkaServers, username, password, serializer, deserializer)

    val streamComplexEvent = env
      .addSource(new FlinkKafkaConsumer[String](topicComplexEvent, schema, properties))
      .map(ComplexEvent(_))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ComplexEvent](Time.seconds(10)) {
        override def extractTimestamp(element: ComplexEvent): Long = element.time
      })

    val streamExtra = env
      .addSource(new FlinkKafkaConsumer[String](topicExtra, schema, properties))
      .map(Extra(_))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Extra](Time.seconds(10)) {
        override def extractTimestamp(element: Extra): Long = element.time
      })

    val joined = (streamComplexEvent join streamExtra)
      .where(_.id).equalTo(_.eventId)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
      .apply((cmp, ext) => cmp.copy(extras = List(ext)))
      .keyBy(_.id)
      .reduce((cpx1, cpx2) => cpx1.copy(extras = cpx1.extras ::: cpx2.extras))

    streamComplexEvent.map(_.toString).print.name("cmpx")
    streamExtra.map(_.toString).print.name("xtra")
    joined.map("joined: " + _.toString).print.name("join")

    env.execute("StreamKafka")
  }
}