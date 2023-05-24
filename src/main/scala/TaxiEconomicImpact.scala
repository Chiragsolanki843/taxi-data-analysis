import TaxiApplication.{bigTaxiDF, taxiZonesDF}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TaxiEconomicImpact {

  def apply(taxiDF: DataFrame, bigTaxiDF: DataFrame, taxiZonesDF: DataFrame)(implicit spark: SparkSession) {

    import spark.implicits._

    val percentGroupAttempt = 0.05 //  5%
    val percentAcceptGrouping = 0.3 // 30%
    val discount = 5 // $5
    val extraCost = 2 // $2
    val avgCostReduction = 0.6 * bigTaxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0) // if we use take(1) it will give array of Double but we need single value for double that's why we only use take(0)
    val percentGroupable = 289623 * 1.0 / 331893


    val groupAttemptsDF = bigTaxiDF
      .select(
    round(unix_timestamp(col("pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
    col("pickup_taxizone_id"),
    col("total_amount"))
      .groupBy(col("fiveMinId"), col("pickup_taxizone_id")) // <- Grouping the trip starting from same location within 5 min time window
      .agg((count("*") * percentGroupable).as("totalTrips"), sum(col("total_amount")).as("total_amount"))
      .orderBy(col("totalTrips").desc_nulls_last)
      .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
      .drop("fiveMinId")
      .join(taxiZonesDF, col("pickup_taxizone_id") === col("LocationID"))
      .drop("LocationID", "service_zone")

    //groupAttemptsDF.show()


    val groupingEstimateEconomicImpactDF = groupAttemptsDF
      .withColumn("groupedRides", col("totalTrips") * percentGroupAttempt)
      .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
      .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
      .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

    //groupingEstimateEconomicImpactDF.show(100)

    val totalEconomicImpactDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("totalImpact"))

    totalEconomicImpactDF.show()

    /*  Best proposal worth ~100000000 dollars in economic impact

    +--------------------+
    |         totalImpact|
    +--------------------+
    |1.3946987545441452E8|
    +--------------------+
     */
  }
}
