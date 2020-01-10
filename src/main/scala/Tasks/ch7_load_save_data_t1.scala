package Tasks

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

import scala.util.matching.Regex


case class AccDic(timeStamp: String, level: String, ip: String, user: String, method: String, path: String, protocol: String)

//cos://big-data-education.iba-gomel-2/users/olagutin/...
object ch7_load_save_data_t1 {
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
      .collect { case m => (m.group("timestamp"), (m.group("level"), m.group("ip"), m.group("user"), m.group("method"), m.group("path"), m.group("protocol"))) }

  }
/*
  es_write_conf = {
    "es.nodes" : "gmlclusterm01.gomel.iba.by",
    "es.port" : "9200",
    "es.resource" : "{$user_id}_spark_access_log/logline",
    "es.mapping.id" : "timestamp",
    "es.write.operation" : "",
    "es.index.auto.create" : "yes",
    "es.nodes.wan.only": "true"
  }*/
}
