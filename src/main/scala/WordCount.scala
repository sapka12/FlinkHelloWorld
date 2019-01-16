import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val text = env.readTextFile(params.get("input"))

    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(params.get("output"), "\n", " ")

    env.execute("Scala WordCount Example")
  }
}
