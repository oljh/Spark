package Tasks

import org.apache.spark.sql.SparkSession
//import sys.process._

object ch8_advanced_program_t3 {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("trDig").master("local").getOrCreate()


    val nums: List[Integer] = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    spark.sparkContext.parallelize(nums).foreach(line => println(s"$line") )

    val trDgShPath = "/src/res/scripts/translate_digit.sh"

  }


}