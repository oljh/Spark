package Tasks

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex


case class Log(timeStamp: String, level: String, className: String, threadName: String, massage: String)

object SparkRddTask2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("SpRdd")
    val sc = new SparkContext(conf)
    val logFile = sc.textFile("src/res/hive-server.log")
    val timeStampRgx = "(?<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})"
    val levelRgx = "(?<level>(?>INFO|ERROR|WARN|TRACE|DEBUG|FATAL))"
    val classRgx = "(?<class>.+?):"
    val threadRgx = "\\[(?<thread>[^]]+)\\]:"
    val messageRgx = "(?<message>[\\s\\S]*?(?=\\d{4}-\\d{2}|\\Z))"
    val keyValPattern: Regex = (timeStampRgx + "\\s+" + levelRgx + "\\s+" + classRgx + "\\s+" + threadRgx + "\\s+" + messageRgx).r


    val logsRDD = logFile
      .flatMap(keyValPattern.findAllMatchIn(_))
      .map(m => Log(m.group("timestamp"), m.group("level"), m.group("class"), m.group("thread"), m.group("message")))

    val warnCnt = logsRDD.filter(f => f.level.contains("WARN")).count()
    val errCnt = logsRDD.filter(f => f.level.contains("ERROR")).count()
    println("Warnings: " + warnCnt + " Errors: " + errCnt + " Total: " + logsRDD.count())
  }

}
