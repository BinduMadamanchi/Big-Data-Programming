import org.apache.spark._
import org.apache.log4j.{Level, Logger}

object Friends{
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)   //setting logger level
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("FacebookFriends").setMaster("local[*]");
    val scontext = new SparkContext(conf)

    def FacebookMapper(line: String) = {   //mapper function which produces key value pairs
      val splittedwords = line.split(" ")
      val key = splittedwords(0)
      val keyvaluepairs = splittedwords.slice(1, splittedwords.size).map(friend => {
        if (key < friend) (key, friend) else (friend, key)
      })
      keyvaluepairs.map(pair => (pair, splittedwords.slice(1, splittedwords.size).toSet))
    }

    def FacebookReducer(accumulator: Set[String], set: Set[String]) = {  // This is a reducer function that groups data by key.
      accumulator intersect set                // Accumulator intersects the data and finds the mutual friends
    }
    val file = scontext.textFile("C:\\Users\\saile\\Desktop\\BINDU\\input.txt")
    val results = file.flatMap(FacebookMapper)
      .reduceByKey(FacebookReducer)
      .filter(!_._2.isEmpty)
      .sortByKey()

    results.collect.foreach(line => {
      println(s"${line._1} , (${line._2.mkString(" ")})")})
    results.coalesce(1).saveAsTextFile("Friendslist")  //saving the output text file

  }

}