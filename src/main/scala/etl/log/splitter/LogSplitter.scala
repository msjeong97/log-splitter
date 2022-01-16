package etl.log.splitter

import org.apache.spark.sql.DataFrame
import etl.common.SplittedLogDFs
import etl.log.loader.AdLogLoader


object LogSplitter {
  def splitLog(df: DataFrame): SplittedLogDFs = {
    val adLogDF = AdLogLoader.parse(df)

    SplittedLogDFs(adLogDF, df, df)
  }
}