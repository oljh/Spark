package Tasks

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex


object ch_key_value_pairs_t3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("SpRdd")
    val sc = new SparkContext(conf)

    val accsFile = sc.textFile("src/res/access.log")
    val accsPattern: Regex = ("^\\[" +
      "(?<timestamp>(\\d{2}\\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\/\\d{4}\\s+\\d{2}:\\d{2}:\\d{2}))\\s+-\\d{4}\\]\\s+" +
      "(?<level>(?>INFO|ERROR|WARN|TRACE|DEBUG|FATAL))\\s+" +
      "(?<ip>((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.?){4})\\s+" +
      "(?<user>.+)\\s+-\\s+\"" +
      "(?<path>(.*?))\\s+" +
      "(?<protocol>(.*?))\"(.+)?$").r

    val accsDataRdd = accsFile
      .flatMap(accsPattern.findAllMatchIn(_))
      .collect { case m if m.group("level").contains("INFO") => (m.group("user"), (m.group("ip"), (m.group("timestamp")))) }

    val usrDataRdd = sc
      .textFile("src/res/passwd")
      .map { line =>
        val separated = line.split(":")
        (separated(0), separated(4))
      }
    val joined = accsDataRdd.leftOuterJoin(usrDataRdd)

      .map { case ((userName, ((ip, timeStamp), fullUserName))) => ((userName, fullUserName, ip), timeStamp) }
      .groupByKey()
      .map { case ((userName, fullUserName, ip), timeStampITr) => (userName, ip, timeStampITr.last, timeStampITr.size) }
      .sortBy(_._4, ascending = false)

    joined.foreach(println(_))

    sc.stop()
  }
}