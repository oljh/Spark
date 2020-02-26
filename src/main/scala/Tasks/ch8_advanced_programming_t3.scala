package Tasks
//import sys.process._
import org.apache.spark.sql.SparkSession

import scala.sys.process.Process
object ch8_advanced_program_t3 {
  /*
  def isWindowsShell = {
    val ostype = System.getenv("OSTYPE")
    val isCygwin = ostype != null && ostype.toLowerCase.contains("cygwin")
    val isWindows = System.getProperty("os.name", "").toLowerCase.contains("windows")
    isWindows && !isCygwin
  }
  private lazy val cmd = if(isWindowsShell) Seq("cmd", "/c", "git") else Seq("git")
*/

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("trDig").master("local").getOrCreate()
    val shFile = "src/res/scripts/translate_digit.sh" // Should be some file on your system
    val shData = spark.read.textFile(shFile).rdd
    shData.coalesce(1).saveAsTextFile("src/res/scripts/translate_digit.sh")

    //System.setProperty("hadoop.home.dir", "C:\\winutil\\")

    val path = shData.saveAsTextFile("file:///src/res/translate_digit.sh")

    val nums2: List[Integer] = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    def bash(command: String) = Process("sh", Seq("-c",s"$command")).!!.trim

    println(bash("echo `date`"))

    println(bash("ls -la"))


    //println(bash("chmod +x src/res/scripts/translate_digit.sh"))





  }


}