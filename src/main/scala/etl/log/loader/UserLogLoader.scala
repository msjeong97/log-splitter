package etl.log.loader

import eu.bitwalker.useragentutils.UserAgent
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StringType}



object UserLogLoader extends LogLoaderBase {
  override def parse(df: DataFrame): DataFrame = {

    val parseUserAgent = udf((uaString: String) => {
      val ua = new UserAgent(uaString)
      val browser = ua.getBrowser.toString
      val os = ua.getOperatingSystem.toString
      val device = ua.getOperatingSystem.getDeviceType.toString

      Map("browser" -> browser, "os" -> os, "device" -> device)
    })


    val loadedDf =
      df.withColumn("city", col("city").cast(StringType))
        .withColumn("country", col("country").cast(StringType))
        .withColumn("ip", col("ip").cast(StringType))
        .withColumn("user_group", col("user_group").cast(IntegerType))
        .withColumn("os", parseUserAgent(col("ua")).getItem("os"))
        .withColumn("device", parseUserAgent(col("ua")).getItem("device"))
        .withColumn("browser", parseUserAgent(col("ua")).getItem("browser"))


    loadedDf.select("city", "country", "ip", "user_group", "os", "device", "browser")
  }
}