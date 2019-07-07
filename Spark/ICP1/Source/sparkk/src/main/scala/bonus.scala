package com.demo
import org.apache.spark.{SparkContext, SparkConf}

object bonus {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val scontext=new SparkContext(sparkConf)

    val input =  scontext.textFile("input2.txt")
    val output = "output2"

    val wordsInFile = input.flatMap(line => line.split(""))

    wordsInFile.foreach(f=>println(f))

    val count = wordsInFile.map(words => (words, 1)).reduceByKey(_+_,1)

    val wordsList=count.sortBy(outputLIst=>outputLIst._1,ascending = true)

    wordsList.foreach(outputLIst=>println(outputLIst))
    wordsList.saveAsTextFile(output)

    wordsList.take(10).foreach(outputLIst=>println(outputLIst))
    scontext.stop()

  }

}