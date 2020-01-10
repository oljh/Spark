package Tasks

object SparkRddTask1 {

  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("SpRdd")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("src/res/README.md")
    val words = textFile.flatMap(_.split("\\W+")).filter(_.length>=1)
    println(words.count())
    sc.stop()
  }
}
