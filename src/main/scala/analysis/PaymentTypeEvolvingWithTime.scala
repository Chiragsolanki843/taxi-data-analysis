package analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// 7- How is the payment type evolving with time?
object PaymentTypeEvolvingWithTime {

  def apply(taxiDF: DataFrame, taxiZonesDF: DataFrame) = {

    val ratecodeEvolution = taxiDF
      .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_date"),col("RatecodeID"))
      .agg(count("*").as("totalTrips"))
      .orderBy(col("pickup_date"))

    ratecodeEvolution.show(31)
  }

  /* TODO --> only two days data
    +-----------+----------+----------+
  |pickup_date|RatecodeID|totalTrips|
  +-----------+----------+----------+
  | 2018-01-25|         1|    260473|
  | 2018-01-25|         2|      5052|
  | 2018-01-25|         5|       628|
  | 2018-01-25|         4|       137|
  | 2018-01-25|        99|         6|
  | 2018-01-25|         6|         3|
  | 2018-01-25|         3|       511|
  | 2018-01-26|         3|        19|
  | 2018-01-26|         2|       826|
  | 2018-01-26|         4|        56|
  | 2018-01-26|         5|       267|
  | 2018-01-26|         1|     63914|
  | 2018-01-26|        99|         1|
  +-----------+----------+----------+
   */
}
