import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.twitter._
import org.apache.spark.{ SparkContext, SparkConf }

object Twitter {

  val conf = new SparkConf().setMaster("local[4]").setAppName("Tweets Extraction")
  val scontext = new SparkContext(conf)

  def main(args: Array[String]) {

    scontext.setLogLevel("WARN") //logger level is set to warn
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // authorization keys and the tokens
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val scontext1 = new StreamingContext(scontext, Seconds(3))     // The spark streaming context is set to a time interval of 3 sec
    val streaming = TwitterUtils.createStream(scontext1, None, filters)
    val allwords = streaming.flatMap(status => status.getText.split(" ").filter(_.startsWith("#"))) // splitting the streaming data based on # hash tags
    val keyvaluepairs = allwords.map(word => (word, 1))
    val wordCounts = keyvaluepairs.reduceByKey(_ + _)   // To count each word in each and every set
    wordCounts.print() // to print the first 10 elements of each RDD generated
    scontext1.start()
    scontext1.awaitTermination()

  }

}