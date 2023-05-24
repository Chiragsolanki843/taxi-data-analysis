package analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// 8- Can we explore a ride-sharing opportunity by grouping close short trips?

object RideSharingOpportunity {

  def apply(taxiDF: DataFrame, taxiZonesDF: DataFrame)(implicit spark: SparkSession) = {

    // unix_timestamp start from --> 1970-01-01 00:00:00 UTC
    // All the trips that can be grouped by location ID and 5 mins bucket.
    val groupAttemptsDF = taxiDF
      .select(
        round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
        col("PULocationID"),
        col("total_amount"))
      .where(col("passenger_count") < 3) // filter almost two passengers
      .groupBy(col("fiveMinId"), col("PULocationID")) // <- Grouping the trip starting from same location within 5 min time window
      .agg(count("*").as("totalTrips"), sum(col("total_amount")).as("total_amount"))
      .orderBy(col("totalTrips").desc_nulls_last)
      .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
      .drop("fiveMinId")
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .drop("LocationID", "service_zone")

    // groupAttemptsDF.show()

    /* TODO --> Single Day Analysis

    +------------+----------+------------------+--------------------+---------+--------------------+
    |PULocationID|totalTrips|      total_amount|approximate_datetime|  Borough|                Zone|
    +------------+----------+------------------+--------------------+---------+--------------------+
    |         237|       115| 1376.199999999999| 2018-01-25 18:45:00|Manhattan|Upper East Side S...|
    |         236|       110|1308.1399999999985| 2018-01-25 11:35:00|Manhattan|Upper East Side N...|
    |         236|       105|1128.3999999999992| 2018-01-25 18:35:00|Manhattan|Upper East Side N...|
    |         237|       104|1164.9699999999991| 2018-01-25 18:10:00|Manhattan|Upper East Side S...|
    |         142|       103|1393.9899999999984| 2018-01-26 01:40:00|Manhattan| Lincoln Square East|
    |         142|       102|1410.8599999999985| 2018-01-26 01:35:00|Manhattan| Lincoln Square East|
    |         236|       101|1087.0899999999988| 2018-01-25 18:30:00|Manhattan|Upper East Side N...|
    |         237|       100|1215.0499999999988| 2018-01-25 21:55:00|Manhattan|Upper East Side S...|
    |         142|        99|1372.2099999999987| 2018-01-26 01:05:00|Manhattan| Lincoln Square East|
    |         162|        99|1615.1199999999983| 2018-01-25 22:35:00|Manhattan|        Midtown East|
    |         237|        99|1224.8099999999993| 2018-01-25 23:10:00|Manhattan|Upper East Side S...|
    |         161|        97| 1352.659999999999| 2018-01-26 00:05:00|Manhattan|      Midtown Center|
    |         161|        97|1429.0299999999986| 2018-01-25 23:10:00|Manhattan|      Midtown Center|
    |         237|        96|1146.6399999999994| 2018-01-25 22:45:00|Manhattan|Upper East Side S...|
    |         161|        96|1428.9899999999993| 2018-01-25 23:35:00|Manhattan|      Midtown Center|
    |         237|        96| 1108.739999999999| 2018-01-25 18:40:00|Manhattan|Upper East Side S...|
    |         161|        95| 1310.079999999999| 2018-01-25 23:15:00|Manhattan|      Midtown Center|
    |         236|        95|1251.4599999999998| 2018-01-25 11:45:00|Manhattan|Upper East Side N...|
    |         236|        95| 1333.379999999999| 2018-01-25 11:40:00|Manhattan|Upper East Side N...|
    |         162|        94|1420.9399999999987| 2018-01-26 00:55:00|Manhattan|        Midtown East|
    +------------+----------+------------------+--------------------+---------+--------------------+

     */

    /**
     * Within 5 min bucket window we have more than 100 trips staring at same location ID.
     *
     * Incentives people to take a grouped ride, at a discount
     * We produce less emissions - This project has significant potential for environment improvement,
     * we can investigate the possibility of a subsidy from the government or local authorities.
     *
     */

    import spark.implicits._

    val percentGroupAttempt = 0.05 //  5%
    val percentAcceptGrouping = 0.3 // 30%
    val discount = 5 // $5
    val extraCost = 2 // $2
    val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0) // if we use take(1) it will give array of Double but we need single value for double that's why we only use take(0)

    val groupingEstimateEconomicImpactDF = groupAttemptsDF
      .withColumn("groupedRides", col("totalTrips") * percentGroupAttempt)
      .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
      .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
      .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

    groupingEstimateEconomicImpactDF.show(100)

    /*
      +------------+----------+------------------+--------------------+---------+--------------------+------------------+----------------------------------+----------------------------------+------------------+
    |PULocationID|totalTrips|      total_amount|approximate_datetime|  Borough|                Zone|      groupedRides|acceptedGroupedRidesEconomicImpact|rejectedGroupedRidesEconomicImpact|       totalImpact|
    +------------+----------+------------------+--------------------+---------+--------------------+------------------+----------------------------------+----------------------------------+------------------+
    |         237|       115| 1376.199999999999| 2018-01-25 18:45:00|Manhattan|Upper East Side S...|              5.75|                 7.827847922779476|                 8.049999999999999|15.877847922779475|
    |         236|       110|1308.1399999999985| 2018-01-25 11:35:00|Manhattan|Upper East Side N...|               5.5|                 7.487506708745586|                 7.699999999999999|15.187506708745586|
    |         236|       105|1128.3999999999992| 2018-01-25 18:35:00|Manhattan|Upper East Side N...|              5.25|                 7.147165494711696|                              7.35|14.497165494711695|
    |         237|       104|1164.9699999999991| 2018-01-25 18:10:00|Manhattan|Upper East Side S...|               5.2|                 7.079097251904918|                 7.279999999999999|14.359097251904917|
    |         142|       103|1393.9899999999984| 2018-01-26 01:40:00|Manhattan| Lincoln Square East|              5.15|                 7.011029009098141|                              7.21| 14.22102900909814|
    |         142|       102|1410.8599999999985| 2018-01-26 01:35:00|Manhattan| Lincoln Square East|5.1000000000000005|                 6.942960766291362|                 7.140000000000001|14.082960766291363|
    |         236|       101|1087.0899999999988| 2018-01-25 18:30:00|Manhattan|Upper East Side N...| 5.050000000000001|                 6.874892523484585|                              7.07|13.944892523484585|
    |         237|       100|1215.0499999999988| 2018-01-25 21:55:00|Manhattan|Upper East Side S...|               5.0|                 6.806824280677806|                               7.0|13.806824280677805|
    |         142|        99|1372.2099999999987| 2018-01-26 01:05:00|Manhattan| Lincoln Square East|              4.95|                6.7387560378710285|                              6.93|13.668756037871027|
    |         162|        99|1615.1199999999983| 2018-01-25 22:35:00|Manhattan|        Midtown East|              4.95|                6.7387560378710285|                              6.93|13.668756037871027|
    |         237|        99|1224.8099999999993| 2018-01-25 23:10:00|Manhattan|Upper East Side S...|              4.95|                6.7387560378710285|                              6.93|13.668756037871027|
    |         161|        97| 1352.659999999999| 2018-01-26 00:05:00|Manhattan|      Midtown Center|4.8500000000000005|                 6.602619552257472|                              6.79|13.392619552257472|
    |         161|        97|1429.0299999999986| 2018-01-25 23:10:00|Manhattan|      Midtown Center|4.8500000000000005|                 6.602619552257472|                              6.79|13.392619552257472|
    |         237|        96|1146.6399999999994| 2018-01-25 22:45:00|Manhattan|Upper East Side S...| 4.800000000000001|                 6.534551309450695|                 6.720000000000001|13.254551309450695|
    |         161|        96|1428.9899999999993| 2018-01-25 23:35:00|Manhattan|      Midtown Center| 4.800000000000001|                 6.534551309450695|                 6.720000000000001|13.254551309450695|
    |         237|        96| 1108.739999999999| 2018-01-25 18:40:00|Manhattan|Upper East Side S...| 4.800000000000001|                 6.534551309450695|                 6.720000000000001|13.254551309450695|
    |         161|        95| 1310.079999999999| 2018-01-25 23:15:00|Manhattan|      Midtown Center|              4.75|                 6.466483066643916|                6.6499999999999995|13.116483066643916|
    |         236|        95|1251.4599999999998| 2018-01-25 11:45:00|Manhattan|Upper East Side N...|              4.75|                 6.466483066643916|                6.6499999999999995|13.116483066643916|
    |         236|        95| 1333.379999999999| 2018-01-25 11:40:00|Manhattan|Upper East Side N...|              4.75|                 6.466483066643916|                6.6499999999999995|13.116483066643916|
    |         162|        94|1420.9399999999987| 2018-01-26 00:55:00|Manhattan|        Midtown East|               4.7|                 6.398414823837137|                              6.58|12.978414823837138|
    |         236|        94|1056.7099999999991| 2018-01-25 18:50:00|Manhattan|Upper East Side N...|               4.7|                 6.398414823837137|                              6.58|12.978414823837138|
    |         237|        93|1093.5899999999992| 2018-01-25 21:45:00|Manhattan|Upper East Side S...|              4.65|                 6.330346581030359|                              6.51| 12.84034658103036|
    |         237|        92| 1130.079999999999| 2018-01-25 22:05:00|Manhattan|Upper East Side S...|4.6000000000000005|                 6.262278338223582|                              6.44|12.702278338223582|
    |         237|        92|1085.0399999999988| 2018-01-26 00:25:00|Manhattan|Upper East Side S...|4.6000000000000005|                 6.262278338223582|                              6.44|12.702278338223582|
    |         162|        92| 1297.929999999999| 2018-01-26 00:50:00|Manhattan|        Midtown East|4.6000000000000005|                 6.262278338223582|                              6.44|12.702278338223582|
    |         237|        91|1165.6499999999987| 2018-01-25 22:50:00|Manhattan|Upper East Side S...|              4.55|                 6.194210095416803|                 6.369999999999999|12.564210095416803|
    |         142|        91|1262.4299999999992| 2018-01-26 01:10:00|Manhattan| Lincoln Square East|              4.55|                 6.194210095416803|                 6.369999999999999|12.564210095416803|
    |         236|        90| 1129.469999999999| 2018-01-25 11:30:00|Manhattan|Upper East Side N...|               4.5|                 6.126141852610025|                               6.3|12.426141852610025|
    |         161|        90| 1277.169999999998| 2018-01-25 17:40:00|Manhattan|      Midtown Center|               4.5|                 6.126141852610025|                               6.3|12.426141852610025|
    |         237|        90|1044.5099999999993| 2018-01-25 21:20:00|Manhattan|Upper East Side S...|               4.5|                 6.126141852610025|                               6.3|12.426141852610025|
    |         161|        90|1500.0299999999993| 2018-01-25 18:00:00|Manhattan|      Midtown Center|               4.5|                 6.126141852610025|                               6.3|12.426141852610025|
    |         237|        90|1143.3999999999992| 2018-01-25 18:20:00|Manhattan|Upper East Side S...|               4.5|                 6.126141852610025|                               6.3|12.426141852610025|
    |         237|        89|1007.9299999999993| 2018-01-25 11:15:00|Manhattan|Upper East Side S...|              4.45|                 6.058073609803247|                6.2299999999999995|12.288073609803247|
    |         161|        89| 1383.569999999999| 2018-01-26 00:15:00|Manhattan|      Midtown Center|              4.45|                 6.058073609803247|                6.2299999999999995|12.288073609803247|
    |         236|        89|1046.9099999999987| 2018-01-25 18:40:00|Manhattan|Upper East Side N...|              4.45|                 6.058073609803247|                6.2299999999999995|12.288073609803247|
    |         236|        89|1047.0399999999993| 2018-01-25 18:45:00|Manhattan|Upper East Side N...|              4.45|                 6.058073609803247|                6.2299999999999995|12.288073609803247|
    |         237|        88| 1011.319999999999| 2018-01-25 21:30:00|Manhattan|Upper East Side S...|               4.4|                  5.99000536699647|                              6.16| 12.15000536699647|
    |         161|        88|1396.8899999999992| 2018-01-25 18:10:00|Manhattan|      Midtown Center|               4.4|                  5.99000536699647|                              6.16| 12.15000536699647|
    |         237|        88|1115.8099999999993| 2018-01-25 22:35:00|Manhattan|Upper East Side S...|               4.4|                  5.99000536699647|                              6.16| 12.15000536699647|
    |         161|        88| 1277.869999999999| 2018-01-25 23:30:00|Manhattan|      Midtown Center|               4.4|                  5.99000536699647|                              6.16| 12.15000536699647|
    |         237|        88|1001.9899999999993| 2018-01-25 11:20:00|Manhattan|Upper East Side S...|               4.4|                  5.99000536699647|                              6.16| 12.15000536699647|
    |         237|        88|1091.7799999999993| 2018-01-25 22:00:00|Manhattan|Upper East Side S...|               4.4|                  5.99000536699647|                              6.16| 12.15000536699647|
    |         237|        88|1050.4499999999991| 2018-01-26 00:10:00|Manhattan|Upper East Side S...|               4.4|                  5.99000536699647|                              6.16| 12.15000536699647|
    |         230|        87|1220.8999999999987| 2018-01-26 00:25:00|Manhattan|Times Sq/Theatre ...|4.3500000000000005|                 5.921937124189692|                 6.090000000000001|12.011937124189693|
    |         161|        87| 1498.469999999999| 2018-01-25 23:20:00|Manhattan|      Midtown Center|4.3500000000000005|                 5.921937124189692|                 6.090000000000001|12.011937124189693|
    |         236|        87| 983.5399999999994| 2018-01-25 22:25:00|Manhattan|Upper East Side N...|4.3500000000000005|                 5.921937124189692|                 6.090000000000001|12.011937124189693|
    |         236|        87|1072.6199999999994| 2018-01-25 21:35:00|Manhattan|Upper East Side N...|4.3500000000000005|                 5.921937124189692|                 6.090000000000001|12.011937124189693|
    |         161|        86|1190.1299999999992| 2018-01-26 00:20:00|Manhattan|      Midtown Center|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         237|        86| 1013.949999999999| 2018-01-25 12:20:00|Manhattan|Upper East Side S...|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         230|        86|1225.2099999999987| 2018-01-26 00:40:00|Manhattan|Times Sq/Theatre ...|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         237|        86|1022.8299999999991| 2018-01-25 18:25:00|Manhattan|Upper East Side S...|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         161|        86|1322.2599999999986| 2018-01-25 16:15:00|Manhattan|      Midtown Center|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         186|        86| 855.7799999999999| 2018-01-25 10:15:00|Manhattan|Penn Station/Madi...|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         237|        86|1076.2099999999996| 2018-01-25 22:15:00|Manhattan|Upper East Side S...|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         237|        86| 961.6599999999992| 2018-01-25 18:15:00|Manhattan|Upper East Side S...|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         236|        86| 1174.799999999999| 2018-01-25 12:05:00|Manhattan|Upper East Side N...|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         236|        86|1057.0599999999993| 2018-01-25 21:10:00|Manhattan|Upper East Side N...|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         161|        86|1529.3599999999992| 2018-01-25 18:25:00|Manhattan|      Midtown Center|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         162|        86|1332.8299999999995| 2018-01-26 01:00:00|Manhattan|        Midtown East|               4.3|                 5.853868881382913|                              6.02|11.873868881382911|
    |         237|        85| 963.2199999999992| 2018-01-25 11:30:00|Manhattan|Upper East Side S...|              4.25|                 5.785800638576135|                 5.949999999999999|11.735800638576134|
    |         237|        85| 938.7599999999991| 2018-01-25 18:50:00|Manhattan|Upper East Side S...|              4.25|                 5.785800638576135|                 5.949999999999999|11.735800638576134|
    |         142|        85| 1178.099999999999| 2018-01-26 02:55:00|Manhattan| Lincoln Square East|              4.25|                 5.785800638576135|                 5.949999999999999|11.735800638576134|
    |         237|        85|1012.4099999999996| 2018-01-25 21:50:00|Manhattan|Upper East Side S...|              4.25|                 5.785800638576135|                 5.949999999999999|11.735800638576134|
    |         161|        85| 1327.949999999999| 2018-01-25 17:50:00|Manhattan|      Midtown Center|              4.25|                 5.785800638576135|                 5.949999999999999|11.735800638576134|
    |         162|        85|1311.5399999999995| 2018-01-26 01:25:00|Manhattan|        Midtown East|              4.25|                 5.785800638576135|                 5.949999999999999|11.735800638576134|
    |         237|        85| 979.2299999999994| 2018-01-25 12:25:00|Manhattan|Upper East Side S...|              4.25|                 5.785800638576135|                 5.949999999999999|11.735800638576134|
    |         236|        85|1106.8099999999995| 2018-01-25 19:40:00|Manhattan|Upper East Side N...|              4.25|                 5.785800638576135|                 5.949999999999999|11.735800638576134|
    |         162|        84|1260.9699999999991| 2018-01-25 22:25:00|Manhattan|        Midtown East|               4.2|                5.7177323957693575|                              5.88|11.597732395769357|
    |         161|        84|1540.8699999999997| 2018-01-25 18:50:00|Manhattan|      Midtown Center|               4.2|                5.7177323957693575|                              5.88|11.597732395769357|
    |         162|        84| 1218.199999999999| 2018-01-25 22:20:00|Manhattan|        Midtown East|               4.2|                5.7177323957693575|                              5.88|11.597732395769357|
    |         162|        84| 1509.679999999999| 2018-01-26 01:45:00|Manhattan|        Midtown East|               4.2|                5.7177323957693575|                              5.88|11.597732395769357|
    |         237|        84|1036.7399999999993| 2018-01-25 22:30:00|Manhattan|Upper East Side S...|               4.2|                5.7177323957693575|                              5.88|11.597732395769357|
    |         236|        84| 969.0299999999992| 2018-01-25 18:55:00|Manhattan|Upper East Side N...|               4.2|                5.7177323957693575|                              5.88|11.597732395769357|
    |         162|        84| 1314.979999999999| 2018-01-25 21:45:00|Manhattan|        Midtown East|               4.2|                5.7177323957693575|                              5.88|11.597732395769357|
    |         162|        83|1158.3399999999995| 2018-01-25 23:20:00|Manhattan|        Midtown East|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         236|        83|1137.9499999999994| 2018-01-25 21:05:00|Manhattan|Upper East Side N...|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         161|        83| 1205.969999999999| 2018-01-25 17:05:00|Manhattan|      Midtown Center|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         237|        83| 865.5299999999995| 2018-01-25 15:10:00|Manhattan|Upper East Side S...|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         162|        83|1354.3499999999995| 2018-01-26 01:15:00|Manhattan|        Midtown East|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         234|        83| 1279.079999999999| 2018-01-25 21:45:00|Manhattan|            Union Sq|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         161|        83|1216.7199999999993| 2018-01-26 00:00:00|Manhattan|      Midtown Center|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         162|        83| 1252.769999999999| 2018-01-25 21:55:00|Manhattan|        Midtown East|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         237|        83|1062.0599999999997| 2018-01-25 21:40:00|Manhattan|Upper East Side S...|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         237|        83|1018.9699999999993| 2018-01-25 21:15:00|Manhattan|Upper East Side S...|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         162|        83|1251.4699999999993| 2018-01-25 21:35:00|Manhattan|        Midtown East|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         162|        83|1353.7399999999989| 2018-01-26 01:05:00|Manhattan|        Midtown East|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         161|        83|1276.3099999999995| 2018-01-25 17:45:00|Manhattan|      Midtown Center|              4.15|                  5.64966415296258|                5.8100000000000005|11.459664152962581|
    |         236|        82| 948.2499999999992| 2018-01-25 19:10:00|Manhattan|Upper East Side N...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         237|        82|1067.4399999999996| 2018-01-25 22:20:00|Manhattan|Upper East Side S...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         161|        82|            1198.5| 2018-01-25 23:25:00|Manhattan|      Midtown Center|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         237|        82| 835.1499999999995| 2018-01-25 19:10:00|Manhattan|Upper East Side S...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         237|        82| 965.0199999999996| 2018-01-25 15:40:00|Manhattan|Upper East Side S...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         237|        82| 947.2599999999995| 2018-01-25 19:40:00|Manhattan|Upper East Side S...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         237|        82| 868.2799999999991| 2018-01-25 18:05:00|Manhattan|Upper East Side S...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         162|        82|1158.3199999999993| 2018-01-25 21:30:00|Manhattan|        Midtown East|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         237|        82|1028.0899999999992| 2018-01-25 22:40:00|Manhattan|Upper East Side S...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         237|        82| 973.2499999999994| 2018-01-25 19:20:00|Manhattan|Upper East Side S...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         236|        82| 927.6999999999992| 2018-01-25 20:10:00|Manhattan|Upper East Side N...|4.1000000000000005|                 5.581595910155802|                              5.74|11.321595910155803|
    |         162|        81|1248.2499999999993| 2018-01-26 00:20:00|Manhattan|        Midtown East|              4.05|                 5.513527667349022|                 5.669999999999999|11.183527667349022|
    |         237|        81| 918.2099999999991| 2018-01-25 15:35:00|Manhattan|Upper East Side S...|              4.05|                 5.513527667349022|                 5.669999999999999|11.183527667349022|
    +------------+----------+------------------+--------------------+---------+--------------------+------------------+----------------------------------+----------------------------------+------------------+
     */

    val totalAmountDF = groupingEstimateEconomicImpactDF.select(sum(col("total_amount")).as("total_amount"))

    totalAmountDF.show()

    /* total rides amount
    +-----------------+
    |sum(total_amount)|
    +-----------------+
    |4597113.550000005|
    +-----------------+
     */

    val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("profit_amount"))

    totalProfitDF.show()

    /* total profit amount  --> 40k/day = 12 million/year!!! we can save
    +-----------------+
    | sum(totalImpact)|
    +-----------------+
    |39987.73868642742|
    +-----------------+
     */
  }
}
