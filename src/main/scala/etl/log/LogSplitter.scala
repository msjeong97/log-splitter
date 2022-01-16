package etl.log

import org.apache.spark.sql.{DataFrame}
import etl.common.SplittedLogDFs


object LogSplitter {
  def splitLog(df: DataFrame): SplittedLogDFs = {
    val adLogDF = AdLogLoader.parse(df)

    SplittedLogDFs(adLogDF, df, df)
  }
}