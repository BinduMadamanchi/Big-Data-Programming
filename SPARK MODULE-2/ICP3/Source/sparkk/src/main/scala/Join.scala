import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object Join {
  def main(args: Array[String]) {
    setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val scontext = new SparkContext(conf)
    val sqlContext1 = new SQLContext(scontext)
    val file = sqlContext1.read.format("com.databricks.spark.csv").load("/Users/saile/Desktop/survey.csv")
    file.registerTempTable("survey1")
    file.registerTempTable("survey2")
    val sqlquery = sqlContext1.sql("select * from survey1 s1 join survey2 s2 on s1.C3=s2.C3")
    sqlquery.show()
    sqlquery.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("outputjoin")

  }
}