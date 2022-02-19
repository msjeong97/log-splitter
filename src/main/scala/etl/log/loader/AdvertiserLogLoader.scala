package etl.log.loader

import org.apache.spark.sql.DataFrame


object AdvertiserLogLoader extends LogLoaderBase {
  override def parse(df: DataFrame): DataFrame = {

    df
  }
}