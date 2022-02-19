package etl.log.loader

import org.apache.spark.sql.DataFrame


object UserLogLoader extends LogLoaderBase {
  override def parse(df: DataFrame): DataFrame = {

    df
  }
}