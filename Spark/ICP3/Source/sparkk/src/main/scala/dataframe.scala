  import java.lang.System.setProperty
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}

  object dataframe {
    def main(args: Array[String]) {

      setProperty("hadoop.home.dir", "C:\\winutils\\")
      val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
      val sc = new SparkContext(conf)

      val sContext = new SQLContext(sc)
      val file= sContext.read.format("com.databricks.spark.csv").load("/Users/saile/Desktop/survey.csv")
      val savefile= file
        .write.format("com.databricks.spark.csv")
        .save("/Users/saile/Desktop/savedFile")
      file.registerTempTable("survey")

      val sqlquery= sContext.sql("select C13 from survey")
      println("1st Query Executed Successfully")
      sqlquery.show()
    }
  }
