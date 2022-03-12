package etl.log.loader

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, from_unixtime, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, TimestampType}


object AdvertiserLogLoader extends LogLoaderBase {
  override def parse(df: DataFrame): DataFrame = {

    def parseCostMeta(costMeta: Column, field: String): Option[Column] = {

      def getSome(column: Column, key: String): Option[Column] = {
        try {
          column.getItem(key) match {
            case null => None
            case cost => Some(cost.cast(StringType))
          }
        } catch {
          case e: Exception => None
        }
      }

      val parsedValue = try {
        getSome(costMeta, field)
      } catch {
        case e: Exception => None
      }

      parsedValue
    }

    val loadedDf =
      df.withColumn("client_id", col("client_id").cast(IntegerType))
        .withColumn("client_name", col("client_name").cast(StringType))
        .withColumn("campaign_id", col("campaign_id").cast(StringType))
        .withColumn("content_id", col("content_id").cast(StringType))
        .withColumn("channel", col("channel").cast(StringType))
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
        .withColumn("cost_client",
          when(col("cost_meta").isNotNull,
          parseCostMeta(col("cost_meta"), "cost_c").get.cast(DoubleType))
            .otherwise(null))

    loadedDf.select("client_id", "client_name", "campaign_id", "action", "tzoffset", "utc_time",
      "local_time", "bid_price", "win_price", "cost_client")
  }
}