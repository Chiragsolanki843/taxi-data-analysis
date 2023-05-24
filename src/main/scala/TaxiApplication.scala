import analysis.{MostPickupsDropOffs, PaymentTypeEvolvingWithTime, PeakHoursForLongShortTrips, PeakHoursForTaxi, PeoplePayingForLongShortTrips, RideSharingOpportunity, TopPickUpAndDropOffForLongShortTrips, TripDistribution}
import org.apache.spark.sql.SparkSession

object TaxiApplication extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Taxi Application")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val taxiDF = spark.read.load("src/main/resources/yellow_taxi_jan_25_2018")

  // taxiDF.printSchema()
  /*

  root
   |-- VendorID: integer (nullable = true)
   |-- tpep_pickup_datetime: timestamp (nullable = true)
   |-- tpep_dropoff_datetime: timestamp (nullable = true)
   |-- passenger_count: integer (nullable = true)
   |-- trip_distance: double (nullable = true)
   |-- RatecodeID: integer (nullable = true)
   |-- store_and_fwd_flag: string (nullable = true)
   |-- PULocationID: integer (nullable = true)
   |-- DOLocationID: integer (nullable = true)
   |-- payment_type: integer (nullable = true)
   |-- fare_amount: double (nullable = true)
   |-- extra: double (nullable = true)
   |-- mta_tax: double (nullable = true)
   |-- tip_amount: double (nullable = true)
   |-- tolls_amount: double (nullable = true)
   |-- improvement_surcharge: double (nullable = true)
   |-- total_amount: double (nullable = true)

   */
  //println(taxiDF.count()) // 331893 in a single day rides

  val bigTaxiDF = spark.read.load("D:\\Data Engineering\\NYC_taxi_2009-2016.parquet")

  // bigTaxiDF.printSchema()
  //println(bigTaxiDF.count()) // 1382375998 taxi rides in entire data

  /*

  root
   |-- dropoff_datetime: timestamp (nullable = true)
   |-- dropoff_latitude: float (nullable = true)
   |-- dropoff_longitude: float (nullable = true)
   |-- dropoff_taxizone_id: integer (nullable = true)
   |-- ehail_fee: float (nullable = true)
   |-- extra: float (nullable = true)
   |-- fare_amount: float (nullable = true)
   |-- improvement_surcharge: float (nullable = true)
   |-- mta_tax: float (nullable = true)
   |-- passenger_count: integer (nullable = true)
   |-- payment_type: string (nullable = true)
   |-- pickup_datetime: timestamp (nullable = true)
   |-- pickup_latitude: float (nullable = true)
   |-- pickup_longitude: float (nullable = true)
   |-- pickup_taxizone_id: integer (nullable = true)
   |-- rate_code_id: integer (nullable = true)
   |-- store_and_fwd_flag: string (nullable = true)
   |-- tip_amount: float (nullable = true)
   |-- tolls_amount: float (nullable = true)
   |-- total_amount: float (nullable = true)
   |-- trip_distance: float (nullable = true)
   |-- trip_type: string (nullable = true)
   |-- vendor_id: string (nullable = true)
   |-- trip_id: long (nullable = true)


    // tpep_pickup_datetime = pickup timestamp
    // tpep_dropoff_datetime = dropoff timestamp
    // passenger_count
    // trip_distance = length of the trip in miles
    // RateCodeId = 1(standard), +2(JFK), 3(Newark), 4 (Nassau/Westchester) or 5 (negotiated)
    // PULocationID = pickup location zone ID
    // DOLocationID = dropoff location zone ID
    // payment_type = 1 (credit card), 2 (cash), 3 (no charge), 4 (dispute), 5 (unknown), 6 (voided), 99=???
    // total_amount
    */

  val taxiZonesDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/taxi_zones.csv")

  //taxiZonesDF.printSchema()

  /*

  root
   |-- LocationID: integer (nullable = true)
   |-- Borough: string (nullable = true)
   |-- Zone: string (nullable = true)
   |-- service_zone: string (nullable = true)
   */


  // 1
  //MostPickupsDropOffs(taxiDF, taxiZonesDF)

  // 2
  //PeakHoursForTaxi(taxiDF, taxiZonesDF)

  // 3
  //TripDistribution(taxiDF, taxiZonesDF)

  // 4
  //PeakHoursForLongShortTrips(taxiDF,taxiZonesDF)

  // 5
  //TopPickUpAndDropOffForLongShortTrips(taxiDF, taxiZonesDF)

  // 6
  //PeoplePayingForLongShortTrips(taxiDF, taxiZonesDF)

  // 7
  //PaymentTypeEvolvingWithTime(taxiDF, taxiZonesDF)

  // 8
  //RideSharingOpportunity(taxiDF, taxiZonesDF)

}
