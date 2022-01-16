package etl.log

import org.apache.spark.sql.DataFrame


object AdLogLoader extends LogLoaderBase {
  override def parse(df: DataFrame): DataFrame = {

    df
  }
}