package stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    env
      .readTextFile("file:///home/arnold/hello.txt")
      .filter(_.startsWith("A"))
      .map((_, "hello"))
      .writeAsCsv("file:///home/arnold/stream-out.txt")

    env.execute("StreamWordCount")

  }

}
