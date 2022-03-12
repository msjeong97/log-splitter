package etl.log.loader

import eu.bitwalker.useragentutils.UserAgent
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{col, element_at, udf}
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
        .withColumn("county", col("county").cast(StringType))
        .withColumn("ip", col("ip").cast(StringType))
        .withColumn("user_group", col("user_group").cast(IntegerType))
        .withColumn("ua_map", parseUserAgent(col("ua")))
        .withColumn("os", element_at(col("ua_map"), "os"))
        .withColumn("browser", element_at(col("ua_map"), "browser"))
        .withColumn("device", element_at(col("ua_map"), "device"))

    loadedDf.select("city", "county", "ip", "user_group", "os", "device", "browser")
  }
}