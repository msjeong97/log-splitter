package etl.common

import org.apache.spark.sql.DataFrame


case class SplittedLogDFs(adLog: DataFrame,
                          recoLog: DataFrame,
                          userLog: DataFrame)