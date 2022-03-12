package etl.log.loader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, TimestampType}


object MediaLogLoader extends LogLoaderBase {
  override def parse(df: DataFrame): DataFrame = {

    val loadedDf =
      df.withColumn("service_id", col("service_id").cast(IntegerType))
        .withColumn("service_name", col("service_name").cast(StringType))
        .withColumn("service_type", col("service_type").cast(StringType))
        .withColumn("action", col("action").cast(StringType))
        .withColumn("tzoffset", col("tzoffset").cast(IntegerType))
        .withColumn(
          "utc_time",
          from_unixtime(col("timestamp").cast(LongType) / 1000)
            .cast(TimestampType))
        .withColumn(
          "local_time",
          from_unixtime((col("timestamp").cast(LongType) / 1000) + (col("tzoffset") * 60))
            .cast(TimestampType))
        .withColumn("")

    loadedDf.select("service_id", "service_name", "service_type", "action", "tzoffset", "local_time",
      "utc_time")
  }
}