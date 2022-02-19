package etl.log.loader

import org.apache.spark.sql.DataFrame


abstract class LogLoaderBase {
  def parse(df: DataFrame): DataFrame
}