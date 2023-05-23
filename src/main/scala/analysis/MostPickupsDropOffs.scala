package analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
// Which zones have the most pickUps/dropOffs overall?

object MostPickupsDropOffs {

  def apply(taxiDF: DataFrame, taxiZonesDF: DataFrame): Unit = {

    val pickupsByTaxiZoneDF = taxiDF
      .groupBy("PULocationID")
      .agg(count("*").as("totalTrips"))
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .drop("service_zone", "LocationID")
      .orderBy(col("totalTrips").desc_nulls_last)
    pickupsByTaxiZoneDF.show()

    /*
    +------------+----------+---------+--------------------+
    |PULocationID|totalTrips|  Borough|                Zone|
    +------------+----------+---------+--------------------+
    |         237|     15945|Manhattan|Upper East Side S...|
    |         161|     15255|Manhattan|      Midtown Center|
    |         236|     13767|Manhattan|Upper East Side N...|
    |         162|     13715|Manhattan|        Midtown East|
    |         170|     11702|Manhattan|         Murray Hill|
    |         234|     11488|Manhattan|            Union Sq|
    |         230|     11455|Manhattan|Times Sq/Theatre ...|
    |         186|     10319|Manhattan|Penn Station/Madi...|
    |          48|     10091|Manhattan|        Clinton East|
    |         163|      9845|Manhattan|       Midtown North|
    |         142|      9810|Manhattan| Lincoln Square East|
    |         138|      9009|   Queens|   LaGuardia Airport|
    |         107|      8045|Manhattan|            Gramercy|
    |         239|      7908|Manhattan|Upper West Side S...|
    |         164|      7896|Manhattan|       Midtown South|
    |         141|      7744|Manhattan|     Lenox Hill West|
    |          68|      7733|Manhattan|        East Chelsea|
    |          79|      7344|Manhattan|        East Village|
    |         100|      7171|Manhattan|    Garment District|
    |         238|      6764|Manhattan|Upper West Side N...|
    +------------+----------+---------+--------------------+
     */

    // 1-b group by borough
    val pickupsByBorough = pickupsByTaxiZoneDF
      .groupBy(col("Borough"))
      .agg(sum(col("totalTrips")).as("totalTrips"))
      .orderBy(col("totalTrips").desc_nulls_last)

    pickupsByBorough.show()

    /*
    +-------------+----------+
    |      Borough|totalTrips|
    +-------------+----------+
    |    Manhattan|    304266|
    |       Queens|     17712|
    |      Unknown|      6644|
    |     Brooklyn|      3037|
    |        Bronx|       211|
    |          EWR|        19|
    |Staten Island|         4|
    +-------------+----------+
     */

    /**
      We see that data is Extremely skewed towards Manhattan
      Proposal --> differentiate prices according to the pick-up/drop-off area, and by demand
      We can slightly increase the prices for Manhattan as its popular
      and slightly decrease the prices at other Borough as there by incentivize that other places in new york to use cab more
     */


  }
}

