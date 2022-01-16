// scalastyle:off println
package etl.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import etl.log.LogSplitter


object OrgLogSplit {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkLogSplitter")
      .getOrCreate()

    val logPath = spark.conf.getOption("spark.etl.log_path").get
    val outputPath = spark.conf.getOption("spark.etl.output_path").get

    val df = spark.read
      .format("json")
      .load(logPath)

    val splitLogDFs = LogSplitter.splitLog(df)

    println(splitLogDFs.adLog.columns.mkString(" "))
    spark.stop()
  }
}
// scalastyle:on println
