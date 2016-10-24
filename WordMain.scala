/**
  * Created by sreeram on 9/11/16.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordMain {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setMaster("local").setAppName("wordcount")

    val sc = new SparkContext(conf)
    val txtdata = sc.textFile("mydata")
    val result = txtdata.flatMap(_.split(" ")).map(words => (words,1)).reduceByKey(_+_)
    result.collect.foreach(println)
    result.saveAsTextFile("output")

  }


}
