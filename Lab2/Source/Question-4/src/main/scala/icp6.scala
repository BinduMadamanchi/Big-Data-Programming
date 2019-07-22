import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._

object icp6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("PageRankAlgorithm")
    val scontext = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("PAGE RANK")
      .config(conf =conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)     //setting the logger levels
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val edges_dataframe = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saile\\Desktop\\BINDU\\nashville-meetup\\group-edges.csv")

    val groups_dataframe = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saile\\Desktop\\BINDU\\nashville-meetup\\meta-groups.csv")

    edges_dataframe.printSchema()        // Printing the Schema
    groups_dataframe.printSchema()

    edges_dataframe.createOrReplaceTempView("edges")  //creating the views inorder to create the edges and vertices
    groups_dataframe.createOrReplaceTempView("groups")
    val groups1 = spark.sql("select * from groups")

    val edges1 = spark.sql("select * from edges")

    // creating the vertices and edges from groups1 and edges1 with the mandatory fields like id,src and dest
    val vertices = groups1
      .withColumnRenamed("group_id", "id").limit(100)
      .distinct()

    val edges = edges1
      .withColumnRenamed("group1", "src").limit(500).distinct()
      .withColumnRenamed("group2", "dst").limit(500).distinct()

    // creating the graph by using the vertices and edges
    val graph = GraphFrame(vertices, edges)

    edges.cache()
    vertices.cache()
    graph.vertices.show()      //displaying the vertices and the edges of the graph
    graph.edges.show()

    println("Total Number of vertices: " + graph.vertices.count)
    println("Total Number of edges: " + graph.edges.count)

    // calculating the page rank as asked in the question
    val calculatePageRank = graph.pageRank.resetProbability(0.15).tol(0.01).run()
    calculatePageRank.vertices.show()
    calculatePageRank.edges.show()
  }

}