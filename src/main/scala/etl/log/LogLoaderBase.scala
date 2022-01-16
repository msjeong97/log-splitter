package etl.log

import org.apache.spark.sql.DataFrame


abstract class LogLoaderBase {
  def parse(df: DataFrame): DataFrame
}