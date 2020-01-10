package Tasks

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex


case class LogInfo(timeStamp: String, level: String, ip: String, user: String, method:String, path: String, protocol:String)

object ch_key_value_pairs_t2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("SpRdd")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("src/res/access.log")

    val keyValPattern: Regex = ("^\\[(?<timestamp>(\\d{2}\\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\/\\d{4}\\s+\\d{2}:\\d{2}:\\d{2}\\s+-\\d{4})\\])\\s+(?<level>(?>INFO|ERROR|WARN|TRACE|DEBUG|FATAL))\\s+(?<ip>((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.?){4})\\s+(?<user>.+)\\s+-\\s+\"(?<method>(POST|HEAD|PUT|GET|DELETE))\\s+(?<path>(.*?))\\s+(?<protocol>(.*?))\"$").r
    val textFileParsed = textFile.flatMap(keyValPattern.findAllMatchIn(_))
      .map(m => LogInfo(m.group("timestamp"), m.group("level"), m.group("ip"), m.group("user"), m.group("method"), m.group("path"), m.group("protocol")))


    val result = textFileParsed.filter(_.level.contains("INFO"))
        .map{case(logInfo) => ((logInfo.method,logInfo.path),1)}
        .reduceByKey(_ + _)
        .sortBy(_._2,ascending = false)
        .map{case ((m,p),i) => (m,(p,i))}
        .groupBy(_._1)
        .map{case (m,pi) => m -> pi.toList.take(5)}
        .flatMap{case (_, pi) => pi.map(f=>f)}

    result.map(f=> println(f._1 +" \t "+f._2._1 +" \t "+f._2._2)).collect().foreach(println)

    sc.stop()
  }
}
/*
.combineByKey(
(v) => (v, 1),
(acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
(acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
).map{ case (key, value) => (key, value._1) }
*/