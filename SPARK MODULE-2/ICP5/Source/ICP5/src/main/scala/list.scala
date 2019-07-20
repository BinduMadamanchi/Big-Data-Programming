import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._
import org.apache.spark.{SparkConf, SparkContext}

object list {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Graphs")
    val scontext = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graphs")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val trips_df = spark.read  // loading of the data and creating a dataframe.
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saile\\Desktop\\trip.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saile\\Desktop\\station.csv")

    trips_df.printSchema()

    station_df.printSchema()


    // create temp views
    trips_df.createOrReplaceTempView("Trips")

    station_df.createOrReplaceTempView("Stations")
    val station = spark.sql("select * from Stations").show()

    val trip = spark.sql("select * from Trips").show()

    val stations = spark.sql("select * from Stations")

    val trips = spark.sql("select * from Trips")
    val concat = spark.sql("select concat(lat,long) from Stations").show()

    val stationVertices = stations
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = trips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    tripEdges.createOrReplaceTempView("trip")
    val commondestination = spark.sql("select dst,count(dst) from trip group by dst  limit 3").show()

    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + trips.count)

    stationGraph.vertices.show()

    stationGraph.edges.show()




    val inDegree = stationGraph.inDegrees

    println("InDegree" + inDegree.orderBy(desc("inDegree")).limit(5))
    inDegree.show(5)

    val outDegree = stationGraph.outDegrees
    println("OutDegree" + outDegree.orderBy(desc("outDegree")).limit(5))
    outDegree.show(5)

    inDegree.createOrReplaceTempView("in")
    outDegree.createOrReplaceTempView("out")


    val bonusQuestion4 = spark.sql("select i.id,i.inDegree,o.outDegree from in i join out o on i.id=o.id")
    bonusQuestion4.createOrReplaceTempView("bonus4")
    val bonusQuestion5 = spark.sql("select id from bonus4 order by inDegree asc,outDegree desc limit 1").show()


    val ver = stationGraph.degrees
    ver.show(5)
    println("Degree" + ver.orderBy(desc("Degree")).limit(5))

    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")

    motifs.show()

    val sourceCount = trips_df.distinct.groupBy("Start Station")
      .agg(count("*").alias("connecting_count"))
      .withColumnRenamed("Start Station", "id")

    val destinationCount = station_df.distinct.groupBy("name")
      .agg(count("*").alias("connecting_count"))
      .withColumnRenamed("name", "id")

    val degrees = sourceCount.union(destinationCount)
      .groupBy("id")
      .agg(sum("connecting_count").alias("degree"))
    degrees.sort("id").show(5, false)

    stationGraph.vertices.write.csv("C:\\Users\\saile\\Desktop\\two.csv")

    stationGraph.edges.write.csv("C:\\Users\\saile\\Desktop\\one.csv")


  }
}

