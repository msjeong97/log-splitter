package etl.log.splitter

import org.apache.spark.sql.DataFrame
import etl.common.SplittedLogDFs
import etl.log.loader.ClientLogLoader


object LogSplitter {
  def splitLog(df: DataFrame): SplittedLogDFs = {
    val clientLogDF = ClientLogLoader.parse(df)

    SplittedLogDFs(clientLogDF, df, df)
  }
}