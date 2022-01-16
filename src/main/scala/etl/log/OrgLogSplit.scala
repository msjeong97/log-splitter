// scalastyle:off println
package etl.log

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object OrgLogSplit {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkLogSplitter")
      .getOrCreate()

    val logPath = spark.conf.getOption("spark.etl.log_path").get
    val outputPath = spark.conf.getOption("spark.etl.output_path").get
    val outputPartitions = spark.conf.get("spark.etl.output_partition").toInt

    val df = spark.read
      .format("json")
      .load(logPath)

    val splitLogDFs = LogSplitter.splitLog(df)

    val adLogDF = splitLogDFs.adLog
    val recoLogDF = splitLogDFs.recoLog
    val userLogDF = splitLogDFs.userLog

    adLogDF.coalesce(outputPartitions)
      .write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(outputPath + "/adLog/")

    recoLogDF.coalesce(outputPartitions)
      .write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(outputPath + "/recoLog/")

    userLogDF.coalesce(outputPartitions)
      .write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(outputPath + "/userLog/")

    spark.stop()
  }
}
// scalastyle:on println
