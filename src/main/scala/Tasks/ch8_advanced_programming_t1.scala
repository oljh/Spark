package Tasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

case class LogP(timeStamp: String, level: String, className: String, threadName: String, massage: String)

object ch8_advanced_program_t1 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("ap").master("local").getOrCreate()
    val logFile = spark.read.textFile("src/res/hive-server.log").rdd

    val accNPrs = new LongAccumulator
    spark.sparkContext.register(accNPrs, "NoParsed")

    val keyValPattern: String =
      "^(?<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+" +
        "(?<level>(?>INFO|ERROR|WARN|TRACE|DEBUG|FATAL))\\s+" +
        "(?<class>.+?):\\s+\\[" +
        "(?<thread>[^]]+)\\]:\\s+" +
        "(?<message>[\\s\\S]*?)$"


   logFile.foreach(line => {
      if (!line.matches(keyValPattern)) {
        accNPrs.add(1)
      }
    })

    val logsRDD = logFile.flatMap(line => keyValPattern.r.findAllMatchIn(line))
      .map(m => LogP(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)))
/*

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path("src/res/temp"), true)
*/


     val warnCnt = logsRDD.filter(f => f.level.contains("WARN")).count()
     val errCnt = logsRDD.filter(f => f.level.contains("ERROR")).count()

    println("Warnings: " + warnCnt + " Errors: " + errCnt + " Total: " + logsRDD.count()+ ", Not parsed:"+ accNPrs.value )

  }
}
