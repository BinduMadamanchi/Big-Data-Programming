import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j._
import org.apache.spark


object SQL3 {


  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("sql")
    val scontext = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("SqlQueries")
      .config(conf = conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR) // setting the logger level
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val table0 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saile\\Desktop\\Bindu\\ks-projects-201801.csv")

    val structschema = StructType(
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

    val table1 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED").schema(structschema)
      .load("C:\\Users\\saile\\Desktop\\Bindu\\ks-projects-201612.csv")

    val table2 = spark.read
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

    val query1 = spark.sql("select ID,name,category from table00 where category='Food'")
    query1.show()
    val query2 = spark.sql("select ID,name,category from table00 where deadline like '%/2013'")
    query2.show()

    val query3 = spark.sql("select count(state) from table00 where state='successful'")
    query3.show()

    val query4 = spark.sql("select ID,name,category,usd_pledged_real,usd_goal_real from table00 " +
      "where usd_pledged_real=usd_goal_real order by category")
    query4.show()

    val query5 = spark.sql("select ID,name,category from table00 where state='canceled'")
    query5.show()


    val query6 = spark.sql("select distinct(category) from table00")
    query6.show()

    val query7 = spark.sql("select currency,count(currency) from table00 group by currency")
    query7.show()

    val query8 = spark.sql("select name,ID from table00 where state='failed' and category='Music' limit 20")
    query8.show()

    val query9 = spark.sql("select name,ID from table00 where name like 'Support%'")
    query9.show()

    val query10 = spark.sql("select a.usd_pledged_real,b.name from table00 a join table11 b on (a.ID=b.ID)")
    query10.show()
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

    // using Spark SQL

    val venued = spark.sql("select Year,Country,Third from WorldCup where usd_pledged_real = usd_goal_real").show()

    val query14 = spark.sql("Select name,ID from table22 where usd_pledged_real > '5000' ")
    query14.show()

    table1.collect.foreach(println)
    )
    table2.filter(conditionExpr = "usd_pledged_real > 5000" ).show( numRows = 5)

    spark.sql(sqlText = "Select name,ID from table22 where usd_pledged_real > '5000' ").show(
  }
}