import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object Union {
  def main(args: Array[String]) {
    setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val scontext = new SparkContext(conf)
    val sqlContext = new SQLContext(scontext)
    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saile/Desktop/survey.csv")
    val save1= file
      .write.format("com.databricks.spark.csv")
      .save("/Users/saile/Desktop/SavedUnionOutput")
    file.registerTempTable("survey")
    val sqlquery= sqlContext.sql("select * from survey where C13 like '%Yes' union select * from survey where C13 like '%No' order by C3")
    println("Union Query Executed!")
    sqlquery.show()
  }
}