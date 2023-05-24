package analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// 2 What are the peak hours for taxi?
object PeakHoursForTaxi {

  def apply(taxiDF: DataFrame, taxiZonesDF: DataFrame): Unit = {

    val pickupsByHourDF = taxiDF
      .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
      .groupBy("hour_of_day")
      .agg(count("*").as("total_Trips"))
      .orderBy(col("total_Trips").desc_nulls_last)

    pickupsByHourDF.show()

    /*
    +-----------+----------+
    |hour_of_day|totalTrips|
    +-----------+----------+
    |         22|     22108|
    |         21|     20924|
    |         23|     20903|
    |          1|     20831|
    |          0|     20421|
    |         18|     18316|
    |         11|     18270|
    |         12|     17983|
    |          2|     17097|
    |         19|     16862|
    |         17|     16741|
    |         20|     16638|
    |         15|     16194|
    |         13|     15988|
    |         16|     15613|
    |         14|     15162|
    |         10|     11964|
    |          3|     10856|
    |          9|      5358|
    |          4|      5127|
    +-----------+----------+
     */

    /**
     * We see that there are clear peak hours with increased demand
     * Proposal: differentiate prices according to demand
     *
     *
     */
  }
}
