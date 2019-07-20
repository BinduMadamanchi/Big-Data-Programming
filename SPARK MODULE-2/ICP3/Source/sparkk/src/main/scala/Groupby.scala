  import java.lang.System.setProperty
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}
  object Groupby {
    def main(args: Array[String]) {
      setProperty("hadoop.home.dir", "C:\\winutils\\")
      val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
      val scontext = new SparkContext(conf)
      val sqlContext = new SQLContext(scontext)
      val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saile/Desktop/survey.csv")
      val saved= file
        .write.format("com.databricks.spark.csv")
        .save("/Users/saile/Desktop/savedgroupby1")
      file.registerTempTable("survey")
      val sqlquery= sqlContext.sql("select C7,count(*) from survey group by C7")
      //println(query executed successfully")
      sqlquery.show()

    }
  }