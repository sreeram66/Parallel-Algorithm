import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by sreeram on 9/4/16.
  */
object word  {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("wordapp")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("mydata")

  }
}