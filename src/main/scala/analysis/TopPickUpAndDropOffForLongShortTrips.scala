package analysis

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

// 5 What are the top 3 pick up and drop off zones for long/short trips?
object TopPickUpAndDropOffForLongShortTrips {

  def apply(taxiDF: DataFrame, taxiZonesDF: DataFrame) = {

    val longDistanceThreshold = 30
    val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)

    def pickupDropoffPopularityDF(predicate: Column) = tripsWithLengthDF
      .where(predicate)
      .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "Pickup_Zone")
      .drop("LocationID", "Borough", "service_zone")
      .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "Dropoff_Zone")
      .drop("LocationID", "Borough", "service_zone")
      .drop("PULocationID", "DOLocationID")
      .orderBy(col("totalTrips").desc_nulls_last)

    pickupDropoffPopularityDF(col("isLong")).show()

    /* TODO --> For LongTrips
    +----------+--------------------+--------------------+
    |totalTrips|         Pickup_Zone|        Dropoff_Zone|
    +----------+--------------------+--------------------+
    |        14|         JFK Airport|                  NA|
    |         8|   LaGuardia Airport|                  NA|
    |         4|         JFK Airport|      Newark Airport|
    |         4|         JFK Airport|         JFK Airport|
    |         3|       Midtown South|                  NA|
    |         3|                  NV|                  NV|
    |         2|         JFK Airport|Riverdale/North R...|
    |         2|        Clinton East|                  NA|
    |         2|   LaGuardia Airport|      Newark Airport|
    |         2|       Midtown North|      Newark Airport|
    |         2|Penn Station/Madi...|                  NA|
    |         1|   LaGuardia Airport|Schuylerville/Edg...|
    |         1|         JFK Airport|Van Nest/Morris Park|
    |         1|         JFK Airport|         Fort Greene|
    |         1|                  NA|Upper East Side S...|
    |         1| Little Italy/NoLiTa|Charleston/Totten...|
    |         1|         JFK Airport|Eltingville/Annad...|
    |         1|         JFK Airport|       Arden Heights|
    |         1|            Flushing|                  NA|
    |         1|         JFK Airport|Prospect-Lefferts...|
    +----------+--------------------+--------------------+
     */



    pickupDropoffPopularityDF(not(col("isLong"))).show()

    /* TODO --> For ShortTrips

      +----------+--------------------+--------------------+
    |totalTrips|         Pickup_Zone|        Dropoff_Zone|
    +----------+--------------------+--------------------+
    |      5558|                  NV|                  NV|
    |      2425|Upper East Side S...|Upper East Side N...|
    |      1962|Upper East Side N...|Upper East Side S...|
    |      1944|Upper East Side N...|Upper East Side N...|
    |      1928|Upper East Side S...|Upper East Side S...|
    |      1052|Upper East Side S...|      Midtown Center|
    |      1012|Upper East Side S...|        Midtown East|
    |       987|      Midtown Center|Upper East Side S...|
    |       965|Upper West Side S...|Upper West Side N...|
    |       882|      Midtown Center|      Midtown Center|
    |       865|     Lenox Hill West|Upper East Side N...|
    |       850|Penn Station/Madi...|      Midtown Center|
    |       828|Upper West Side N...|Upper West Side S...|
    |       824|Upper West Side S...| Lincoln Square East|
    |       809| Lincoln Square East|Upper West Side S...|
    |       808|     Lenox Hill West|Upper East Side S...|
    |       797|        Midtown East|         Murray Hill|
    |       784|Upper East Side S...|     Lenox Hill West|
    |       763|      Yorkville West|Upper East Side N...|
    |       757|Times Sq/Theatre ...|Penn Station/Madi...|
    +----------+--------------------+--------------------+

     */

    /**
     * There is clear separation of long/short trips
     *
     * Short trips in between wealthy zones like Bars, Restaurants
     *
     * Long trips mostly used for airport transfers
     *
     * Proposal : To the NYC town hall : airport rapid transit
     *
     * for the taxi company : separate market segments and tailor services to each
     *
     * Strike a partnerships with bars/restaurants for pickup service
     */

  }
}
