package etl.log.splitter

import org.apache.spark.sql.DataFrame
import etl.common.SplittedLogDFs
import etl.log.loader.AdvertiserLogLoader
import etl.log.loader.MediaLogLoader
import etl.log.loader.UserLogLoader


object LogSplitter {
  def splitLog(df: DataFrame): SplittedLogDFs = {
    val advertiserLogDF = AdvertiserLogLoader.parse(df)
    val mediaLogDF = MediaLogLoader.parse(df)
    val userLogDF = UserLogLoader.parse(df)

    SplittedLogDFs(advertiserLogDF, mediaLogDF, userLogDF)
  }
}