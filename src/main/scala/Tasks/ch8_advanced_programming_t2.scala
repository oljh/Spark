package Tasks

import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex


object ch8_advanced_program_t2 {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("ap").master("local").getOrCreate()
    val accsFileRDD = spark.read.textFile("src/res/access.log").rdd

    val pswFileRDD = spark.read.textFile("src/res/passwd").rdd


    val accsPattern: Regex = ("^\\[" +
      "(?<timestamp>(\\d{2}\\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\/\\d{4}\\s+\\d{2}:\\d{2}:\\d{2}))\\s+-\\d{4}\\]\\s+" +
      "(?<level>(?>INFO|ERROR|WARN|TRACE|DEBUG|FATAL))\\s+" +
      "(?<ip>((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.?){4})\\s+" +
      "(?<user>.+)\\s+-\\s+\"" +
      "(?<path>(.*?))\\s+" +
      "(?<protocol>(.*?))\"(.+)?$").r

    val accsDataRdd = accsFileRDD
      .flatMap(accsPattern.findAllMatchIn(_))
      .collect { case m if m.group(4).contains("INFO") => (m.group(8), (m.group(5), (m.group(2)))) }


    val pswDataRdd = pswFileRDD
      .map { line =>
        val separated = line.split(":")
        (separated(0), separated(4))
      }

    val pswDataRdd_BC = spark.sparkContext.broadcast(pswDataRdd.collectAsMap())

    val joinResult = accsDataRdd.map { case (k, v) => (k, v, pswDataRdd_BC.value.get(k)) }.persist()
      .map { case ((userName, (ip, timeStamp), fullUserName)) => ((userName, fullUserName, ip), timeStamp) }
      .groupByKey()
      .map { case ((userName, fullUserName, ip), timeStampITr) => (userName, ip, timeStampITr.last, timeStampITr.size) }
      .sortBy(_._4, ascending = false)

    joinResult.foreach(println)
  }


}