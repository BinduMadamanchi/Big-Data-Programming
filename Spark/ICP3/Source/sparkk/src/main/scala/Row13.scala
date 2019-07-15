import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object Row13 {
  def main(args: Array[String]) {
    setProperty("hadoop.home.dir", "C:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val scontext = new SparkContext(conf)
    val sqlContext = new SQLContext(scontext)
    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saile/Desktop/survey.csv")
    file.registerTempTable("survey")
    val row = file.rdd.take(13).last
    print(row)
  }
}