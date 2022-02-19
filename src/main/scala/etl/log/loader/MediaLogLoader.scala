package etl.log.loader

import org.apache.spark.sql.DataFrame


object MediaLogLoader extends LogLoaderBase {
  override def parse(df: DataFrame): DataFrame = {

    df
  }
}