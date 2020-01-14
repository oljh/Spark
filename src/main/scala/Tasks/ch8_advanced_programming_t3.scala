package Tasks

import org.apache.spark.sql.SparkSession
//import sys.process._
import scala.sys.process._
object ch8_advanced_program_t3 {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("trDig").master("local").getOrCreate()
    val shFile = "src/res/scripts/translate_digit.sh" // Should be some file on your system
    val shData = spark.read.textFile(shFile).cache()


    val nums: List[Integer] = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)




    def bashShell(command: String) = Seq("bash", "-c", s"$command").!!.trim
    //spark.sparkContext.parallelize(br.lines()).foreach(line => println(s"$line") )

    println(bashShell("chmod +x src/res/scripts/translate_digit2.sh"))

    println(bashShell("./src/res/scripts/translate_digit2.sh"))

    println(bashShell("echo `date`"))
  }


}