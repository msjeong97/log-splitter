// scalastyle:off println
package etl.log.splitter

import org.apache.spark.sql.{SaveMode, SparkSession}


object OrgLogSplitter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkLogSplitter")
      .getOrCreate()

    val logPath = spark.conf.getOption("spark.etl.log_path").get
    val outputPath = spark.conf.getOption("spark.etl.output_path").get
    val outputPartitions = spark.conf.getOption("spark.etl.output_partition").get.toInt
    val numPartitions = spark.conf.getOption("spark.sql.shuffle.partitions").get.toInt

    val df = spark.read
      .format("json")
      .load(logPath)
      .repartition(numPartitions)

    val splitLogDFs = LogSplitter.splitLog(df)

    val advertiserLogDF = splitLogDFs.adLog
    val mediaLogDF = splitLogDFs.recoLog
    val userLogDF = splitLogDFs.userLog

    advertiserLogDF.coalesce(outputPartitions)
      .write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(outputPath + "/advertiser/")

    mediaLogDF.coalesce(outputPartitions)
      .write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(outputPath + "/media/")

    userLogDF.coalesce(outputPartitions)
      .write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(outputPath + "/user/")

    spark.stop()
  }
}
// scalastyle:on println
