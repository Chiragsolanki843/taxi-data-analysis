package analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// 4 What are the peak hours for long/short trips ?
object PeakHoursForLongShortTrips {

  def apply(taxiDF: DataFrame, taxiZonesDF: DataFrame) = {

    val longDistanceThreshold = 30 // 30 miles

    val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)

    val pickupByHourByLengthDF = tripsWithLengthDF
      .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
      .groupBy("hour_of_day", "isLong")
      .agg(count("*").as("totalTrips"))
      .orderBy(col("totalTrips").desc_nulls_last)

    pickupByHourByLengthDF.show(48)

    /*
    +-----------+------+----------+
    |hour_of_day|isLong|totalTrips|
    +-----------+------+----------+
    |         22| false|     22104|
    |         21| false|     20921|
    |         23| false|     20897|
    |          1| false|     20824|
    |          0| false|     20412|
    |         18| false|     18315|
    |         11| false|     18264|
    |         12| false|     17980|
    |          2| false|     17094|
    |         19| false|     16858|
    |         17| false|     16734|
    |         20| false|     16635|
    |         15| false|     16190|
    |         13| false|     15985|
    |         16| false|     15610|
    |         14| false|     15158|
    |         10| false|     11961|
    |          3| false|     10853|
    |          9| false|      5358|
    |          4| false|      5124|
    |          5| false|      3194|
    |          6| false|      1971|
    |          8| false|      1803|
    |          7| false|      1565|
    |          0|  true|         9|
    |         17|  true|         7|
    |          1|  true|         7|
    |         23|  true|         6|
    |         11|  true|         6|
    |         14|  true|         4|
    |         19|  true|         4|
    |         22|  true|         4|
    |         15|  true|         4|
    |         12|  true|         3|
    |          3|  true|         3|
    |         21|  true|         3|
    |         20|  true|         3|
    |          4|  true|         3|
    |         13|  true|         3|
    |          2|  true|         3|
    |         10|  true|         3|
    |         16|  true|         3|
    |          6|  true|         2|
    |          5|  true|         2|
    |         18|  true|         1|
    +-----------+------+----------+
     */
  }
}