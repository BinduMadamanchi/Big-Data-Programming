
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}


object SQL1 {


  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("sql")
    val scontext = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("SqlQueries")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)    // setting the logger level
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val table0 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saile\\Desktop\\Bindu\\ks-projects-201801.csv")

    val structschema=StructType(
      StructField("ID", IntegerType, true) ::
        StructField("name", StringType, true) ::
        StructField("category", StringType, true) ::
        StructField("main_category", StringType, true) ::
        StructField("currency", StringType, true) ::
        StructField("deadline", StringType, true) ::
        StructField("goal", IntegerType, true) ::
        StructField("pledged", IntegerType, true) ::
        StructField("state", StringType, true) ::
        StructField("usd_pledged_real", IntegerType, true) ::
        StructField("usd_goal_real", IntegerType, true) :: Nil)

    val table1=spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED").schema(structschema)
      .load("C:\\Users\\saile\\Desktop\\Bindu\\ks-projects-201612.csv")

    val table2=spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED").schema(structschema)
      .load("C:\\Users\\saile\\Desktop\\Bindu\\ks-projects-201612.csv")


    // Printing the Schema

    table0.printSchema()

    table1.printSchema()

    table2.printSchema()

    //b.Perform   10   intuitive   questions   in   Dataset
    //For this problem we have used the Spark SqL on DataFrames

    //First of all creat three Temp View

    table0.createOrReplaceTempView("table00")
    table1.createOrReplaceTempView("table11")
    table2.createOrReplaceTempView("table22")

    val csv = scontext.textFile("C:\\Users\\saile\\Desktop\\Bindu\\ks-projects-201612.csv")

    val header = csv.first()

    val data = csv.filter(line => line != header)

    val rdd = data.map(line=>line.split(",")).collect()


    val venue = data.filter(line => line.split(",")(1)==line.split(",")(4))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(4)))
      .collect()

    venue.foreach(println)

    // Using Dataframe

    table1.select("name","usd_pledged_real","usd_goal_real").filter("usd_pledged_real==usd_goal_real").show(10)

    // usig Spark SQL

    val venued = spark.sql("select Year,Country,Third from WorldCup where usd_pledged_real = usd_goal_real").show()


  }

}
