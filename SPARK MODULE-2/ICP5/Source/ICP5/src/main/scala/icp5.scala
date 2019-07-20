import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._
import org.apache.spark.{SparkConf, SparkContext}
object icp5 {

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

    val trips_df = spark.read
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


    val stations = spark.sql("select * from Stations")

    val trips = spark.sql("select * from Trips")

    val stationVertices = stations
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = trips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")


    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + trips.count)//

    stationGraph.vertices.show()

    stationGraph.edges.show()




    val inDegree = stationGraph.inDegrees

    println("InDegree" + inDegree.orderBy(desc("inDegree")).limit(5))
    inDegree.show(5)

    val outDegree = stationGraph.outDegrees
    println("OutDegree" + outDegree.orderBy(desc("outDegree")).limit(5))
    outDegree.show(5)


    val vertices = stationGraph.degrees
    vertices.show(5)
    println("Degree" + vertices.orderBy(desc("Degree")).limit(5))

    val motif = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")

    motif.show()

    stationGraph.vertices.write.csv("C:\\Users\\saile\\Desktop\\two.csv")

    stationGraph.edges.write.csv("C:\\Users\\saile\\Desktop\\one.csv")


  }
}