package analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// 6- How are people paying for the rides, on long / short trips
object PeoplePayingForLongShortTrips {

  def apply(taxiDF: DataFrame, taxiZonesDF: DataFrame) = {

    val ratecodeDistributionDF = taxiDF
      .groupBy("RatecodeID")
      .agg(count("*").as("totalTrips"))
      .orderBy(col("totalTrips").desc_nulls_last)

    ratecodeDistributionDF.show()

    /*
    +----------+----------+
    |RatecodeID|totalTrips|
    +----------+----------+
    |         1|    324387|
    |         2|      5878|
    |         5|       895|
    |         3|       530|
    |         4|       193|
    |        99|         7|
    |         6|         3|
    +----------+----------+
     */
    /**
     *  as per the analysis we know Cash is dying!
     *
     *  Make sure the card payment processor works 24/7
     *
     *
     */

  }

}
