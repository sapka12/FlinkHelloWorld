name := "FlinkHelloWorld"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.7.1"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.7.1"
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % "1.7.1"
libraryDependencies += "com.lightbend" %% "kafka-streams-scala" % "0.0.1"
