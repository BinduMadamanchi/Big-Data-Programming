import org.apache.spark.{ SparkConf, SparkContext }

object secondarySorting {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    val conf = new SparkConf().setAppName("SECONDARYSORTING").setMaster("local[*]")
    val scontext = new SparkContext(conf)

    val perRDD = scontext.textFile("input1.txt")
    val pairsRDD = perRDD.map(_.split(",")).map { m => ((m(0), m(1)),m(2))}
    println("PAIRS")
    pairsRDD.foreach { println }
    val numberOfReducers = 4;

    val listRDD = pairsRDD.groupByKey(numberOfReducers).mapValues(iter => iter.toList.sortBy(k => k))
    println("LIST")


    listRDD.saveAsTextFile("output1");

  }
}