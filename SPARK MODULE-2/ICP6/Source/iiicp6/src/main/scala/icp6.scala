import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object icp6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("GraphFrames")
    val scontext = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("GraphFrames")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val trips = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saile\\Desktop\\trip.csv")

    val stations = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saile\\Desktop\\station.csv")



    // Printing the Schema

    trips.printSchema()

    stations.printSchema()


    trips.createOrReplaceTempView("Trips")

    stations.createOrReplaceTempView("Stations")


    val station = spark.sql("select * from Stations")

    val trip = spark.sql("select * from Trips")

    val stationVertices = station
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = trip
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    val stationsGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()
    stationsGraph.vertices.show()
    stationsGraph.edges.show()
    // Triangle Count
    val stationTriCount = stationsGraph.triangleCount.run()
    stationTriCount.select("id","count").show()
    // Shortest Path
    val shortestPath = stationsGraph.shortestPaths.landmarks(Seq("Golden Gate at Polk","MLK Library")).run
    shortestPath.show()
    //Page Rank
    val stationsPageRank = stationsGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationsPageRank.vertices.select("id", "pagerank").show()
    stationsPageRank.edges.show()
    // BFS
    val pathBFS = stationsGraph.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount < 10").run()
    pathBFS.show()

    //Saving to File
    stationsGraph.vertices.write.csv("C:\\Users\\saile\\Desktop\\one.csv")

    stationsGraph.edges.write.csv("C:\\Users\\saile\\Desktop\\two.csv")








  }

}
