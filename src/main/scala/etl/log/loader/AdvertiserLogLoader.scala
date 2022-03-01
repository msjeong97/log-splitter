package etl.log.loader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, TimestampType}


object AdvertiserLogLoader extends LogLoaderBase {
  override def parse(df: DataFrame): DataFrame = {

    df.withColumn("client_id", col("client_id").cast(IntegerType))
      .withColumn("campaign_id", col("campaign_id").cast(StringType))
      .withColumn("action", col("action").cast(StringType))
      .withColumn("tzoffset", col("tzoffset_c").cast(IntegerType))
      .withColumn(
        "utc_time",
        from_unixtime(col("timestamp").cast(LongType) / 1000)
          .cast(TimestampType))
      .withColumn(
        "local_time",
        from_unixtime((col("timestamp").cast(LongType) / 1000) + (col("tzoffset") * 60))
          .cast(TimestampType))
      .withColumn("bid_price", col("bid_price").cast(DoubleType))
      .withColumn("win_price", col("win_price").cast(DoubleType))
  }
}