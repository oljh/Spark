package Tasks

import org.apache.spark.{SparkConf, SparkContext}

object ch_key_value_pairs_t1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("SpRdd")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("src/res/README.md")
    val words = textFile
      .map(_.toLowerCase)
      .flatMap("\\w+".r.findAllIn(_))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2,ascending = false)
      .take(5)
    words.foreach(f => println("The word '" + f._1 + "' appears " + f._2 + " times"))
    sc.stop()
  }
}
