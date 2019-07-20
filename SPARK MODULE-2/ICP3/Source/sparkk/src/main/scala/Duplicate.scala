import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Duplicate {
  def main(args: Array[String]) {

    setProperty("hadoop.home.dir", "C:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val scontext = new SparkContext(conf)
    val sqlContext = new SQLContext(scontext)

    // loading the tweetfile
    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saile/Desktop/survey.csv")
    val table1=file.registerTempTable("survey")
    val table2=file.registerTempTable("survey1")

    val sqlquery = sqlContext.sql("select COUNT(),C3 from survey GROUP By C3 Having COUNT() > 1")
    sqlquery.show()
   }
}