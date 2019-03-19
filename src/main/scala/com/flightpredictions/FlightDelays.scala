package com.flightpredictions

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset,DataFrame, SparkSession}
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object FlightDelays {

  val conf: SparkConf = new SparkConf().setAppName("Flight Delays").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("Flight Delays")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate()

  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("dofW", IntegerType, true),
    StructField("carrier", StringType, true),
    StructField("origin", StringType, true),
    StructField("dest", StringType, true),
    StructField("crsdephour", IntegerType, true),
    StructField("crsdeptime", DoubleType, true),
    StructField("depdelay", DoubleType, true),
    StructField("crsarrtime", DoubleType, true),
    StructField("arrdelay", DoubleType, true),
    StructField("crselapsedtime", DoubleType, true),
    StructField("dist", DoubleType, true)
  ))

  case class Flight(_id: String,
                    dofW: Integer,
                    carrier: String,
                    origin: String,
                    dest: String,
                    crsdephour: Integer,
                    crsdeptime: Double,
                    depdelay: Double,
                    crsarrtime: Double,
                    arrdelay: Double,
                    crselapsedtime: Double,
                    dist: Double) extends Serializable

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val log = LogManager.getRootLogger
    log.setLevel(Level.ERROR)

    val file = "/Users/manikandanragavan/Documents/mr-flight-delay-predictions/data/flights20170102.json"

    val df : Dataset[Flight]= spark.read.format("json").option("inferSchema", "false").schema(schema).load(file).as[Flight]

    // cache DataFrame in columnar format in memory
    df.cache

//    df.count()

    // create Table view of DataFrame for Spark SQL
    df.createOrReplaceTempView("flights")

    // cache flights table in columnar format in memory
    spark.catalog.cacheTable("flights")
    df.show(false)

//    println(" filter flights that departed at 10 AM. take 3")
//    val crs10 = df.filter(flight => flight.crsdephour == 10).take(3)
//    crs10.foreach(println)

    // group by and count by carrier
    println("Group by and count by carrier ")
    df.groupBy("carrier").count().show()

    //count the departure delays greater than 40 minutes by destination, and sort them with the highest first.
    println("Count the departure delays greater than 40 minutes by destination, and sort them with the highest first")
    df.filter($"depdelay" > 40).groupBy("dest").count().orderBy(desc("count")).show(3)

    // top 5 dep delay
    println("Longest departure delays ")
    df.select($"carrier", $"origin", $"dest", $"depdelay", $"crsdephour",$"dist", $"dofW").filter($"depdelay" > 40).orderBy(desc("depdelay")).show(5)
//    spark.sql("select carrier,origin, dest, depdelay,crsdephour, dist, dofW from flights where depdelay > 40 order by depdelay desc limit 5").show

    println("Average departure delay by Carrier")
    df.groupBy("carrier").agg(avg("depdelay").as("avgdelay")).show

    println("Average departure delay by day of the week")
    df.groupBy("dofW").agg(avg("depdelay").as("avgdelay")).orderBy(desc("avgdelay")).show
//    spark.sql("SELECT dofW, avg(depdelay) as avgdelay FROM flights GROUP BY dofW ORDER BY avgdelay desc").show

    //Count of Departure Delays by Carrier (where delay >40 minutes)
    println("Count of Departure Delays by Carrier ")
    df.filter($"depdelay" > 40).groupBy("carrier").count().orderBy(desc("count")).show

    println("Count of departure delay by origin airport where delay minutes >40")
    df.filter($"depdelay" > 40).groupBy("origin").count().orderBy(desc("count")).show

    // Count of Departure Delays by Day of the week
    println("Count of Departure Delays by dofW ")
    df.filter($"depdelay" > 40).groupBy("dofW").count().orderBy(desc("count")).show

    println("Count of Departure Delays by scheduled departure hour")
    df.groupBy("crsdephour").count().orderBy(desc("count")).show

    println("Count of Departure Delays by origin")
    df.filter($"depdelay" > 40).groupBy("origin").agg(count("depdelay").as("depdelay")).orderBy(desc("depdelay"))show()
//    spark.sql("select origin,count(depdelay) from flights group by origin").show




  }
}
